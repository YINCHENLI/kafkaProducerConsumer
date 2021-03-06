package com.yinchen.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Spark Streaming 处理socket数据
  *
  * 测试 nc
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    //local[?] ?have to be > 2
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //需要两个参数，一个是SparkConf和一个batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}