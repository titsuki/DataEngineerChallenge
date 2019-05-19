import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

object AvgDurationCalcurator {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("AvgDurationCalcurator").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val records = sc.textFile("sessionized-log").map { case line =>
      val Array(ip, idx, _, date) = line.split("\t")
      ((ip, idx), date)
    }
    val durationSum: Double = records.aggregateByKey(Set.empty[String])((s,v) => s + v, (s1,s2) => s1 ++ s2).map { case (session, dateSet) =>
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      val dateList = dateSet.toSeq.sortBy{ case myKey => format.parse(myKey).getTime }
      val firstTimestamp = format.parse(dateList.head).getTime
      val lastTimestamp = format.parse(dateList.last).getTime
      lastTimestamp - firstTimestamp
    }.sum
    val sessionCount: Double = records.count.toDouble
    sc.makeRDD[Double](Seq( durationSum / sessionCount )).saveAsTextFile("duration-avg")
  }
}
