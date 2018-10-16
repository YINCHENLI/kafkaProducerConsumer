package com.yinchen.spark.project.dao

import com.yinchen.spark.project.domain.{CourseClickCount, CourseSearchClickCount}
import com.yinchen.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseSearchClickCountDAO {

  val tableName = "course_search_clickcount_imooc"
  val cf = "info"
  val qualifier = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]) : Unit = {

    //using java and scala at the same time
    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      //对row key对应的数字来自动加入, this is a great API that why we choose Hbase
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count
      )
    }

  }

  /**
    * 根据rowkey 查询值
    * @param day_search_course
    */
  def count(day_search_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get  = new Get(Bytes.toBytes(day_search_course))

    val value = table.get(get).getValue(cf.getBytes, qualifier.getBytes)

    //the first time you count, the value is null
    if (value == null){
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args : Array[String]) : Unit = {

    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20181015_www.baidu.com_8", 8))
    list.append(CourseSearchClickCount("20181015_search.yahoo.com_9", 9))
    list.append(CourseSearchClickCount("20181015_www.bing.com_10", 150))

    save(list)

    println(count("20181015_www.baidu.com_8") + " : " + + count("20181015_search.yahoo.com_9") + " : " + count("20181015_www.bing.com_10"))
  }


}
