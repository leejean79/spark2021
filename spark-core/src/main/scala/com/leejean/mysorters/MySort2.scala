package com.leejean.mysorters
/*
该代码进一步演示了传入rdd方法sortBy（）中的是一个比较规则！！
尽管它是以封装到一个类里面。
所以，我可以将元祖的一部分传入到该类中
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MySort2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("mysort2")
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

    val sortedRdd: RDD[(String, Int, Int)] = userRdd.sortBy(tp => new Boy(tp._2, tp._3))

    println(sortedRdd.collect().toBuffer)



  }
  }

//定义一个类，继承比较器和序列化
class Boy(val age:Int, val fv:Int) extends Ordered[Boy] with Serializable {
  override def compare(that: Boy): Int = {
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

}
