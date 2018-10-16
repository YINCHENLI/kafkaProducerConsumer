package com.yinchen.spark.project.domain

/**
  * 从搜索引擎过来的course click
  * @param day_search_course
  * @param click_count
  */
case class CourseSearchClickCount(day_search_course : String, click_count: Long)
