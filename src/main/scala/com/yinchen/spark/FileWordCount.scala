package com.yinchen.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    //no need to use receiver so local[?] can be any number
    val sparkConf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
    //需要两个参数，一个是SparkConf和一个batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //spark streaming will moinitor the directory data and process any files created in that directory
    //have to have the sane data format
    val lines = ssc.textFileStream("file://somedirectorypath")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
