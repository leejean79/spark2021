package com.leejean.iplocation2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/*
broadcastjoin的演示，将小容量的规则进行封装后进行广播，
 */

object IpLocationSql {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("IpLocationSql").getOrCreate()

    //将规则进行广播
    val rDS: Dataset[String] = spark.read.textFile(args(0))
    import spark.implicits._
    //将规则文件封装为dataset
    val rulesDS: Dataset[(Long, Long, String)] = rDS.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })

    //收集到driver端内存
    val ruleInDriver: Array[(Long, Long, String)] = rulesDS.collect()
    //封装为广播变量
    val brRule: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(ruleInDriver)

    //将日志封装为df
    val logDs: Dataset[String] = spark.read.textFile(args(1))
    val ipDF: DataFrame = logDs.map(line => {
      //get ip and turn it into Long
      val logArr: Array[String] = line.split("[|]")
      val ipStr: String = logArr(1)
      val ipL: Long = IPUtils.ip2Long(ipStr)
      ipL
    }).toDF("ipNum")

    //注册临时表
    ipDF.createTempView("v_ipNum")

    //自定义sql函数，并注册
    spark.udf.register("ip2Province", (ip: Long) => {
      val rule: Array[(Long, Long, String)] = brRule.value
      val i: Int = IPUtils.binarySearch(rule, ip)
      var provice = "未知"
      if (i != -1) {
        provice = rule(i)._3
      }
      provice

    })

    //执行sql
    val r: DataFrame = spark.sql("select ip2Province(ipNum) province, count(*) counts from v_ipNum group by province order by counts desc")
    
    r.show()

    spark.close()


  }

}
