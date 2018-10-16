package com.yinchen.spark.project.domain

/**
  * info after cleaning
  * @param ip ip address of the access log line
  * @param time the time of visiting
  * @param courseId the id number of the course
  * @param statusCode HTTP statusCode , e.g. 200, 404, 500
  * @param referrer the visiting referrer
  */
case class ClickLog (ip:String, time:String, courseId:Int, statusCode:Int, referrer:String)
