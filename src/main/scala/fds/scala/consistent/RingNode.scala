package fds.scala.consistent

import scala.collection.mutable.Map

/**
  * Most simple local representation of a node on the Ring
  */
class RingNode(val name: String) {
  private val map: Map[String, Any] = Map[String, Any]()

  def put(key: String, value: Any): Unit = {
    map.put(key, value)
  }

  def get(key: String): Option[Any] = map.get(key)

  def remove(key: String): Unit = {
    map.remove(key)
  }

  def size: Int = map.size

  def keys(): Set[String] = map.keys.toSet
}

object RingNode {
  def apply(name: String): RingNode = new RingNode(name)
}