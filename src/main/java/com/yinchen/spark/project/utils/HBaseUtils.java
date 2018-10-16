package com.yinchen.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase 操作工具类，采用单例模式 封装，Singleton
 */
public class HBaseUtils {

    HBaseAdmin admin = null;

    Configuration configuration = null;

    /**
     * 私有构造方法 private constructor
     */
    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.0.101:2181");
        configuration.set("hbase.rootdir", "hdfs://192.168.0.101:8020/hbase");

        try {
          admin = new HBaseAdmin(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public synchronized static HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取到HTable实例
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * add one record to HBase table
     * @param tableName name of table
     * @param rowKey rowkey of hbase table
     * @param cf HBase table's columnfamily
     * @param column HBase table column
     * @param value the value that writes to HBase
     */
    public void put(String tableName, String rowKey, String cf, String column, String value){
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        HTable table = HBaseUtils.getInstance().getTable("course_clickcount_imooc");
//
//        System.out.println(table.getName().getNameAsString());
        String tableName = "course_clickcount_imooc";
        String rowKey = "20181015_88";
        String cf = "info";
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName, rowKey, cf, column, value);
    }


}
