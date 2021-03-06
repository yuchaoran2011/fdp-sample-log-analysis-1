package com.lightbend.fdp.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime

/*
  Log analysis application that processes data in Apache server log format.
  Example datasets can be found in:
  http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html
 */
object LogAnalysis {
  val exampleApacheLogs = List(
    """10.10.10.10 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg
      | HTTP/1.1" 304 315""".stripMargin.lines.mkString,
    """10.10.10.10 - "FRED" [18/Jan/2013:18:02:37 +1100] "GET http://images.com/2013/Generic.jpg
      | HTTP/1.1" 304 306""".stripMargin.lines.mkString
  )

  case class LogFields(ip: String, clientId: String, user: String, dataTime: String, method: String,
                       endpoint: String, protocol: String, status: String, payloadSize: Long)

  def main(args: Array[String]) {

    val appName = "Apache Log Analysis"
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val dataSet = if (args.length == 1) sc.textFile(args(0)) else sc.parallelize(exampleApacheLogs)

    val logRegex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)""".r

    val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" ->7,
      "Aug" -> 8,  "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

    /** Tracks the total query count and number of aggregate bytes for a particular group. */
    case class Stats(count: Int, numBytes: Int) extends Serializable {
      def +(other: Stats): Stats = new Stats(count + other.count, numBytes + other.numBytes)
      override def toString: String = "bytes=%s\tn=%s".format(numBytes, count)
    }

    def extractKey(line: String): Option[(String,String,String)] = {
      logRegex.findFirstIn(line) match {
        case Some(logRegex(ip, _, user, _, method, endpoint, protocol, _, _)) =>
          if (user != "\"-\"") Some(ip, user, method ++ endpoint ++ protocol)
          else Option(null, null, null)
        case _ => Option(null, null, null)
      }
    }

    def extractStats(line: String): Stats = {
      logRegex.findFirstIn(line) match {
        case Some(logRegex(ip, clientId, user, dateTime, method, endpoint, protocol, status, bytes)) =>
          new Stats(1, bytes.toInt)
        case _ => new Stats(1, 0)
      }
    }

    // Convert Apache time format into a Joda datetime object
    def parseApacheTime(s: String): String = {
      (new DateTime(s.substring(7, 11).toInt,
        month_map(s.substring(3, 6)),
        s.substring(0, 2).toInt,
        s.substring(12, 14).toInt,
        s.substring(15, 17).toInt,
        s.substring(18, 20).toInt)).toString()
    }

    /* Parse a line in the Apache Common Log format
       Returns a tuple that is either a dictionary containing the parts of the Apache Access Log and 1,
       or the original invalid log line and 0
    */
    def parseApacheLogLine(line: String) = {
      logRegex.findFirstIn(line) match {
        case Some(logRegex(ip, clientId, user, dateTime, method, endpoint, protocol, status, bytes)) =>
          val size = if (bytes == "-") 0L else bytes.toLong
          (LogFields(ip, clientId, user, dateTime, method, endpoint, protocol, status, size), 1)
        case _ => (line, 0)
      }
    }

    def parseLogs() = {
      val parsedLogs = dataSet.map(parseApacheLogLine)
      val accessLogs = parsedLogs.filter(s => s._2 == 1).map(s => s._1).map(_.asInstanceOf[LogFields])
      val failedLogs = parsedLogs.filter(s => s._2 == 0).map(s => s._1)
      (parsedLogs, accessLogs, failedLogs)
    }

    val (_, accessLogs, _) = parseLogs()
    accessLogs.toDF().registerTempTable("logs")

    val statusCodeToCount = accessLogs.map(log => (log.status, 1)).reduceByKey((a, b) => a + b).cache()
    val statusCodeToCountList = statusCodeToCount.take(100)

    println("Response codes and their counts:")
    statusCodeToCountList.foreach(println)

    val ipCounts = accessLogs.map(log => (log.ip, 1)).reduceByKey((a, b) => a + b)
    val ipMoreThan10 = ipCounts.filter(x => x._2 > 10)
    val ipPick20 = ipMoreThan10.map(x => x._1).take(20)
    println("Any 20 hosts that have accessed more then 10 times:")
    ipPick20.foreach(println)


    val endpointCounts = accessLogs.map(log => (log.endpoint, 1)).reduceByKey((a, b) => a + b)
    val top10points = endpointCounts.takeOrdered(10)(Ordering[Int].on(x => -1 * x._2))
    println("Top 10 endpoints:")
    top10points.foreach(println)

    sc.stop()
  }
}
