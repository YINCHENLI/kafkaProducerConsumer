package com.yinchen.spark.project.domain


/**
  * course click rate
  * @param day_course 对应的是HBase中的RowKey， 20171111_1
  * @param click_count 对应的20171111_1的访问总数
  */
case class CourseClickCount(day_course:String, click_count:Long)
