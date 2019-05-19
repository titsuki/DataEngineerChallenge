import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

object UniqueVisitCalcurator {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("UniqueVisitCalcurator").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("sessionized-log").map { case line =>
      val Array(ip, idx, url, _) = line.split("\t")
      ((ip, idx), url)
    }.reduceByKey((a,b) => a).map { case ((ip,idx),url) => (url, 1) }.reduceByKey(_ + _).map { case (url, count) =>
      List(url, count).mkString("\t")
    }.saveAsTextFile("unique-url-count")
  }
}
