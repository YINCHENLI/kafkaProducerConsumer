package com.yinchen.spark.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {

    //parse the arguments
    if (args.length != 4) {
      println("Usage:ImoocStatStreamingApp <zkQuorum> <groupid> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //we only need the second data 第二个数据

    messages.map(_._2).count().print
    //进行数据清洗的操作
    //1. ip address 我们可以根据地点来统计，本项目并没implement
    ssc.start()
    ssc.awaitTermination()


  }

}
