package com.leejean.onHive

import org.apache.spark.sql.SparkSession

object Test1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val modelNames = Array("FM","FFM","DEEPFM","NFM","DIN","DIEN")
    val modelNamesRdd = spark.sparkContext.parallelize(modelNames,1)
    modelNamesRdd.saveAsTextFile("hdfs://hadoop001:9000/sparktest/modelNames")

  }

}
