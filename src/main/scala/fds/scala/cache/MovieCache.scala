package fds.scala.cache

import java.net.{URL, URLConnection}
import java.util.Scanner

import com.zink.cache.{Cache, CacheFactory}

/**
  * Stub for Movie Cache
  */
object MovieCache extends App {

  val cache: Cache = CacheFactory.connect("127.0.0.1")

  private[cache] val base: String = "https://api.themoviedb.org/3/"
  private[cache] val key: String = "?api_key=???"

  val ids: Seq[String] = getPopularMovieIDs
  System.out.println( ids )
  System.out.println( getDetailsById(ids(0) ) )

  def getPopularMovieIDs: Seq[String] = {
    val pop: String = "movie/popular"
    val json: String = httpGet(base + pop + key)
    val ids = scanForKeysValue("\"id\":", json)
    return ids
  }

  private def scanForKeysValue(pattern: String, target: String): List[String] = {
    val idx: Int = target.indexOf(pattern)
    if (idx == -1) Nil
    else {
      val valueAndTail : String = target.substring(idx + pattern.length)
      val ids: List[String] = valueAndTail :: scanForKeysValue(pattern, valueAndTail)
      return ids.map( x => x.split(",")(0) )
    }
  }

  def getDetailsById(id: String) : String =
    Option(cache.get(id))
      .map(_.toString)
      .getOrElse {
        val movie: String = "movie/"
        val result = httpGet(base + movie + id + key)
        cache.set(id, result)
        result
      }

  private def httpGet(uri: String): String = {
    val conn: URLConnection = new URL(uri).openConnection
    val scr: Scanner = new Scanner(conn.getInputStream)
    scr.useDelimiter("\\Z")
    return scr.next
  }
}
