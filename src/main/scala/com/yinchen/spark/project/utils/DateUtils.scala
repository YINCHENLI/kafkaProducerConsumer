package com.yinchen.spark.project.utils
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time : String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time : String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }
}
