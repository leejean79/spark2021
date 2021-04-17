package com.leejean.sparkSql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object WordCountWithAPI {

  def main(args: Array[String]): Unit = {

    //create sparksession
    val spark: SparkSession = SparkSession.builder().appName("SqlWordCount").master("local[*]").getOrCreate()

    //read the data file
    //返回dataset，它只有1列value
    val lines: Dataset[String] = spark.read.textFile("file:///Users/lijing/Documents/temp/temp")
    //导入隐式转换
    import spark.implicits._
    val splitedDS: Dataset[String] = lines.flatMap(_.split(" "))

    //导入聚合函数
    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = splitedDS.groupBy($"value" as "words").agg(count("*") as "totalNum").sort($"totalNum")

    result.show()

    spark.close()
  }

}
