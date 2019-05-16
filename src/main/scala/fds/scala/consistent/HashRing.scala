package fds.scala.consistent

import java.math.BigInteger

import scala.annotation.tailrec
import scala.util.Random

sealed trait Tree extends Product with Serializable {
  def minIndex(): Int
  def maxIndex(): Int
  def leafNodeCount(): Int
  def add(newIndex: Int, newNode: RingNode): Tree
  def remove(toRemove: RingNode): Option[Tree]
  def find(seekIndex: Int): RingNode
  def isIndexPopulated(seekIndex: Int): Boolean
  def findIndexForNode(nodeToFind: RingNode): Option[Int]
  def foldLeaves[T](initial: T)(f: (T, Tree.Leaf) => T): T
  def forEachLeaf(f: Tree.Leaf => Unit): Unit
}

object Tree {
  case class Leaf(index: Int, node: RingNode) extends Tree {
    def minIndex(): Int = index
    def maxIndex(): Int = index
    def leafNodeCount(): Int = 1

    def add(newIndex: Int, newNode: RingNode): Tree =
      if (newIndex < index) Parent(Leaf(newIndex, newNode), this)
      else if (newIndex == index) this
      else Parent(this, Leaf(newIndex, newNode))

    def remove(toRemove: RingNode): Option[Tree] = if (toRemove == node) None else Some(this)

    def find(seekIndex: Int): RingNode = node

    def isIndexPopulated(seekIndex: Int): Boolean = seekIndex == index

    def findIndexForNode(nodeToFind: RingNode): Option[Int] =
      if (this == nodeToFind) Some(index)
      else None

    def foldLeaves[T](initial: T)(f: (T, Tree.Leaf) => T): T = f(initial, this)

    def forEachLeaf(f: Tree.Leaf => Unit): Unit = f(this)
  }

  case class Parent(left: Tree, right: Tree) extends Tree {
    val minIndex = left.minIndex
    val maxIndex = right.maxIndex

    def leafNodeCount(): Int = left.leafNodeCount + right.leafNodeCount

    def add(newIndex: Int, newNode: RingNode): Tree = {
      def addToLeft = Parent(left.add(newIndex, newNode), right)
      def addToRight = Parent(left, right.add(newIndex, newNode))

      if (newIndex < left.maxIndex) addToLeft
      else if (newIndex > right.minIndex) addToRight
      else if (left.leafNodeCount < right.leafNodeCount) addToLeft
      else addToRight
    }

    def remove(toRemove: RingNode): Option[Tree] = (left.remove(toRemove), right.remove(toRemove)) match {
      case (None, None) => None
      case (None, r: Some[Tree]) => r
      case (l: Some[Tree], None) => l
      case (Some(l), Some(r)) => Some(Parent(l, r))
    }

    def find(seekIndex: Int): RingNode =
      if (seekIndex > left.maxIndex && seekIndex <= right.maxIndex) right.find(seekIndex)
      else left.find(seekIndex)

    def isIndexPopulated(seekIndex: Int): Boolean =
      (seekIndex >= minIndex && seekIndex <= maxIndex) &&
        (left.isIndexPopulated(seekIndex) || right.isIndexPopulated(seekIndex))

    def findIndexForNode(nodeToFind: RingNode): Option[Int] =
      left.findIndexForNode(nodeToFind) orElse right.findIndexForNode(nodeToFind)

    def foldLeaves[T](initial: T)(f: (T, Tree.Leaf) => T): T =
      right.foldLeaves(left.foldLeaves(initial)(f))(f)

    def forEachLeaf(f: Tree.Leaf => Unit): Unit = {
      left.forEachLeaf(f)
      right.forEachLeaf(f)
    }
  }
}

/**
  * Stub for a Consistent hash ring
  */
class HashRing(slots: Int) {
  var nodes: Option[Tree] = None

  def addNode(node: RingNode): Unit = {
    val index = findUnusedIndex
    nodes = Some(nodes match {
      case None => Tree.Leaf(index, node)

      case Some(tree) =>
        val nextNode = tree.find(index)
        val newTree = tree.add(index, node)
        val toMove = nextNode.keys.map(k => (k, indexForKey(k))).filter(_._2 <= index).map(_._1)
        toMove.foreach { k =>
          node.put(k, nextNode.get(k))
          nextNode.remove(k)
        }
        newTree
    })
  }

  def removeNode(node: RingNode): Unit =
    nodes = for {
      tree <- nodes
      index <- tree.findIndexForNode(node)
      newTree <- tree.remove(node)
    } yield {
      val subsequentNode = newTree.find(index)
      node.keys().foreach { k =>
        subsequentNode.put(k, node.get(k))
      }
      newTree
    }

  def put(key: String, value: Any): Unit = {
    findNodeForKey(key).map(_.put(key, value))
  }

  def get(key: String): Option[Any] = {
    findNodeForKey(key).flatMap(_.get(key))
  }

  def size: Int = {
    nodes.fold(0)(_.foldLeaves(0) { _ + _.node.size })
  }

  private def findNodeForKey(key: String): Option[RingNode] = nodes.map(_.find(indexForKey(key)))
  private def indexForKey(key: String): Int = (Hasher.sha1(key) mod BigInteger.valueOf(slots)).intValue

  @tailrec
  private def findUnusedIndex(): Int = {
    val i = Random.nextInt(slots)
    if (nodes.fold(false)(_.isIndexPopulated(i))) findUnusedIndex
    else i
  }
}

object HashRing {
  def apply(slots: Int): HashRing = new HashRing(slots)
}

object HashRingFu extends App {
  val ring = HashRing(16384)

  for {
    n <- 1 to 3
    node = RingNode(s"Node $n")
    _ <- 1 to 5
  } ring.addNode(node)

  printRing(ring)

  (0 until 10000).foreach { i =>
    ring.put(s"key:$i", s"value:$i")
  }

  printRing(ring)

  private def printRing(ring: HashRing): Unit = {
    println("====")
    ring.nodes.foreach(_.forEachLeaf { l => println(show(l)) })
    println("----")
  }

  private def show(leaf: Tree.Leaf): String = s"${leaf.index} : ${show(leaf.node)}"
  private def show(node: RingNode): String = s"${node.name} : ${node.size}"
}