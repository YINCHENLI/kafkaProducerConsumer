package com.yinchen.spark.project.spark

import com.yinchen.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.yinchen.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.yinchen.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

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

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp")//.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //we only need the second data 第二个数据

    //1. 测试步骤1
    // messages.map(_._2).count().print
    //2. 测试步骤2 进行数据清洗的操作
    //ip address 我们可以根据地点来统计，本项目并没implement

    val logs = messages.map(_._2)

    val cleanData = logs.map(line => {
      val infos = line.split("\t")

      //info(2) = "GET /class/130.html HTTP/1.1"
      val url = infos(2).split(" ")(1)
      var courseId = 0

      //we next have the id of the course
      //if the url doesn't start with /class, we filter it
      if(url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt

      }
      //ClickLog (ip:String, time:String, courseId:Int, statusCode:Int, referrer:String)
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //cleanData.print

    // 3.测试步骤3 - 统计今天到现在为止课程的访问量

    cleanData.map(x => {
      //hbase rowkey设计
      (x.time.substring(0,8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {

      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    // 4.测试步骤4 - 统计今天到现在为止从搜索引擎来的课程的访问量
    cleanData.map(x => {
      //https://www.google.com/web?query=SparkSQL
      //=>
      //https:/www.google.com/web....
      val referrer = x.referrer.replaceAll("//","/")
      val splits = referrer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }

      //return
      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {

      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
