package com.leejean.mysorters

/*
将样例类与比较规则分开，这样可以灵活更换规则
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MySort4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("mysort4")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    val strRdd: RDD[String] = sc.parallelize(users)

    val userRdd: RDD[(String, Int, Int)] = strRdd.map(line => {
      val usrArr: Array[String] = line.split(" ")
      val name = usrArr(0)
      val age = usrArr(1).toInt
      val fv = usrArr(2).toInt
      (name, age, fv)
    })

    //导入规则
    import SortRules.MyOrdering
    val sortedRdd: RDD[(String, Int, Int)] = userRdd.sortBy(tp => Teacher(tp._2, tp._3))

    println(sortedRdd.collect().toBuffer)
  }
  }

//定义一个case class类
 case class Teacher(age:Int, fv:Int)

