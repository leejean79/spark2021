package com.leejean.iplocation2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/*
从本地读取ip规则，然后用广播变量传递给每个executor
 */
object IpLocationLocal {

  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf()
//    conf.setAppName("iplocation1").setMaster("local[*]")
//    val sc = new SparkContext(conf)
val spark = SparkSession.builder().master("local[*]").appName("IpLocationLocal").getOrCreate()

    //get the local ip rules
    val rules: Array[(Long, Long, String)] = IPUtils.readRules(args(0))

    //broadcast the rules
    val brRules: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rules)

    //    val logRdd: RDD[String] = sc.textFile(args(1))
    //use spark_sql读取数据
    val lines: Dataset[String] = spark.read.textFile(args(1))
    import spark.implicits._
    val rDS: Dataset[(String, Int)] = lines.map(line => {
      val rulesArr: Array[(Long, Long, String)] = brRules.value
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
    }
    )

    val ipDF: DataFrame = rDS.toDF("province", "num")

    val resutlDF: DataFrame = ipDF.groupBy($"province").count()

    resutlDF.show()

    spark.stop()


  }

}
