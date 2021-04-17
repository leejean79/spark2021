package com.leejean.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
/*
spark数据倾斜的处理：
与MR处理类似，首先重新定义key的构成，生成随机key；
聚合后再进行一次map操作，去掉随机组成，进行二次聚合
 */
object DataLean {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.setAppName("datalean")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textRdd: RDD[String] = sc.textFile("/Users/lijing/Documents/temp/data/text.txt")
    textRdd.flatMap(line => line.split(" "))
      .map((_, 1))
      .map(t => {
        val num = Random.nextInt(4)
        val w = t._1
        (w, 1)
      }).reduceByKey(_+_, 4)
      .map(t =>{
        val w = t._1.split("_")(0)
        (w, t._2)
      }).reduceByKey(_+_,4)
      .saveAsTextFile("/Users/lijing/Documents/temp/data/out")



  }

}
