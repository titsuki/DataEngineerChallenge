import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

object Sessionizer {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("Sessionizer").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("2015_07_22_mktplace_shop_web_log_sample.log.gz").map { case line =>
      val Array(date, _, ipWithPort, _, _, _, _, _, _, _, _, _, url, _*) = line.split(" ")
      val Array(ip, port) = ipWithPort.split(":")
      (ip, (url, date))
    }.aggregateByKey(Set.empty[(String, String)])((s,v) => s + v, (s1,s2) => s1 ++ s2).flatMap { case (ip, urlDateSet) =>
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      var currentTime = 0L
      var idx = 0
      for ((url, dateText) <- urlDateSet.toSeq.sortBy{ case (_, myKey) => format.parse(myKey).getTime } ) yield {
        val timestamp = format.parse(dateText).getTime
        val out = if (currentTime == 0) {
           List(ip, 0, url, dateText).mkString("\t")
        } else {
           val nextTime = timestamp
           if (Math.abs(nextTime - currentTime) <= 1000 * 60 * 30) {
             List(ip, idx, url, dateText).mkString("\t")
           } else {
             idx = idx + 1        
             List(ip, idx, url, dateText).mkString("\t")
           }
        }
        currentTime = timestamp
        out
      }
    }.saveAsTextFile("sessionized-log")
  }
}
