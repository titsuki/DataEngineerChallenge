import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

object EngagementCalcurator {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("EngagementCalcurator").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val records = sc.textFile("sessionized-log").map { case line =>
      val Array(ip, idx, _, date) = line.split("\t")
      ((ip, idx), date)
    }.aggregateByKey(Set.empty[String])((s,v) => s + v, (s1,s2) => s1 ++ s2).map { case (session, dateSet) =>
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      val dateList = dateSet.toSeq.sortBy{ case myKey => format.parse(myKey).getTime }
      val firstTimestamp = format.parse(dateList.head).getTime
      val lastTimestamp = format.parse(dateList.last).getTime
      ((),(session,lastTimestamp - firstTimestamp))
    }.reduceByKey((a,b) => if (a._2 > b._2) a else b).map { case ((),(session, duration)) => List(session._1, duration).mkString("\t") }.saveAsTextFile("most-engaged-user")
  }
}
