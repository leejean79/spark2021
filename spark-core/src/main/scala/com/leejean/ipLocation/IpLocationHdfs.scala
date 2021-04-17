package com.leejean.ipLocation

import java.sql.{Driver, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/*
这里考虑当我的规则非常的大，需要存放在hdfs上，当任务运行时，每个excutor会从hdfs读取规则，
但是，这样会造成每个节点的规则不完整。
 */
//解决办法：各节点读取规则后，collect到driver端，再以广播变量的形式发送出去

object IpLocationHdfs {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("iplocation1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //get rules in hdfs
    val rulesRdd: RDD[String] = sc.textFile(args(0))
    val rules: RDD[(Long, Long, String)] = rulesRdd.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    val rulesArr: Array[(Long, Long, String)] = rules.collect()

    val rulesBr: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesArr)

    val logRdd: RDD[String] = sc.textFile(args(1))

    val result: RDD[(String, Int)] = logRdd.map(line => {
      val rulesArr: Array[(Long, Long, String)] = rulesBr.value
      //get ip and turn it into Long
      val logArr: Array[String] = line.split("[|]")
      val ipStr: String = logArr(1)
      val ipL: Long = IPUtils.ip2Long(ipStr)
      //get binary search
      //return the line number of the result
      var provice = "未知"
      val row: Int = IPUtils.binarySearch(rulesArr, ipL)
      if (row != -1) {
        provice = rulesArr(row)._3
      }
      (provice, 1)
    }).reduceByKey(_ + _)

//    println(ipLocationTuple.toBuffer)

    //spark结果的持久化
    result.foreachPartition(itor => IPUtils.data2Mysql(itor))

    sc.stop()


  }

}
