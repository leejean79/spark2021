package com.leejean.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {

  def main(args: Array[String]): Unit = {
    //创建Spark配置对象
    val conf = new SparkConf();
    conf.setAppName("WordCountSpark")
    //设置master属性
    //conf.setMaster("local") ;

    //通过conf创建sc
    val sc = new SparkContext(conf);

    //加载文本文件
    val rdd1 = sc.textFile(args(0));
    //压扁
    val rdd2 = rdd1.flatMap(line => line.split(" ")) ;
    //映射w => (w,1)
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect()
    r.foreach(println)
  }

}
