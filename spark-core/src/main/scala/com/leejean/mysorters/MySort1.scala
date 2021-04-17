package com.leejean.mysorters

/*
自定义排序：
1、按颜值排序，由大到小
2、颜值一样按年龄排序，生序
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MySort1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("mysort1")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    val strRdd: RDD[String] = sc.parallelize(users)

    val userRdd: RDD[User] = strRdd.map(line => {
      val usrArr: Array[String] = line.split(" ")
      val name = usrArr(0)
      val age = usrArr(1).toInt
      val fv = usrArr(2).toInt
      val user: User = new User(name, age, fv)
      user
    })

    val sortedRdd: RDD[User] = userRdd.sortBy(u => u)

    println(sortedRdd.collect().toBuffer)



  }

  //定义一个类，继承比较器和序列化
  class User(val name:String, val age:Int, val fv:Int) extends Ordered[User] with Serializable {

    override def compare(that: User): Int = {
      //首先比较fv，降序
      if (this.fv != that.fv){
        that.fv - this.fv
      }else{
        //颜值一样，比较age，生序
        this.age - that.age
      }
    }

    //定义一个tostring方法
    override def toString: String = s"name: $name, age: $age, fv: $fv"
  }

}
