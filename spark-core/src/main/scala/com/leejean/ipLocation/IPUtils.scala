package com.leejean.ipLocation

import java.sql.DriverManager

import scala.io.{BufferedSource, Source}

object IPUtils {

  /*
  将spark计算结果持久化。
  在持久化时需要考虑：
  1、所有的计算结果都保存在每个excutor，如果从executor收集到driver后再持久化，经过两次网络传输并且对driver压力大
  2、持久化操作在每个executor进行时，避免重复的创建数据库连接
  3、注意持久化时的驱动
   */
  def data2Mysql(itor: Iterator[(String, Int)]): Unit ={

    val conn = DriverManager.getConnection("jdbc:mysql://hadoop001:3306/bigdata?characterEncoding=UTF-8", "root", "123456")
    val pps = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
    itor.foreach(t => {
      pps.setString(1, t._1)
      pps.setInt(2, t._2)
      pps.executeUpdate()
    })
    pps.close()
    conn.close()

  }


  /*
  将ip地址转换为十进制
   */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /*
  ip规则：
  1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
  获取规则的起始、结束十进制，和省份
   */
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  /*
  二分法查找
   */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    val ip = "47.104.77.11"

    val ipLong: Long = ip2Long(ip)

    val rulesArr: Array[(Long, Long, String)] = readRules("/Users/lijing/Documents/temp/ip.txt")

    val numLine: Int = binarySearch(rulesArr, ipLong)

    if (numLine != -1){
      val province = rulesArr(numLine)._3
      println(province)
    }else{
      println("没有找到")
    }
  }

}
