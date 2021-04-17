package com.leejean.mysorters

/*
用样例类替代普通类，实现规则的传入
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MySort3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("mysort3")
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

    val sortedRdd: RDD[(String, Int, Int)] = userRdd.sortBy(tp => Man(tp._2, tp._3))

    println(sortedRdd.collect().toBuffer)
  }
  }

//定义一个case class类，本身就可以序列化
case class Man(age:Int, fv:Int) extends Ordered[Man] {
  override def compare(that: Man): Int = {
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

}
