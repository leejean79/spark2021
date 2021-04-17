package com.leejean.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
spark_sql2.0进行wordcount
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    //create sparksession
    val spark: SparkSession = SparkSession.builder().appName("SqlWordCount").master("local[*]").getOrCreate()

    //read the data file
    //返回dataset，它只有1列value
    val lines: Dataset[String] = spark.read.textFile("file:///Users/lijing/Documents/temp/temp")
    //导入隐式转换
    import spark.implicits._
    val splitedDS: Dataset[String] = lines.flatMap(_.split(" "))

    //注册临时表
    splitedDS.createTempView("words")

    //sql
    val result: DataFrame = spark.sql("select value word, count(*) num from words group by value order by num desc")

    //action
    result.show()

    spark.close()
  }


}
