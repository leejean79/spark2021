package com.leejean.sparkjdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MySparkJdbcDemo {

  /*
  define a function to get jdbc connection
   */
  val myConn = () => {
    DriverManager.getConnection("jdbc:mysql://hadoop001:3306/big4?characterEncoding=UTF-8", "root", "123456")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MySparkJdbcDemo")
    val sc = new SparkContext(conf)

    //创建jdbcRdd
    /*
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,a function that returns an open Connection.
    sql: String,The query must contain two ? placeholders for parameters used to partition the results.
        select title, author from books where ? <= id and id <= ?
    lowerBound: Long,lowerBound the minimum value of the first placeholder
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
    a function from a ResultSet to a single row of the desired result type(s).
     */
    val jdbcRdd: JdbcRDD[(Int, String, Int)] = new JdbcRDD[(Int, String, Int)](
      sc,
      myConn,
      "select * from users where id >= ? and id <= ?",
      1,
      3,
      2,
      rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }
    )

    val r: Array[(Int, String, Int)] = jdbcRdd.collect()

    println(r.toBuffer)

    sc.stop()

  }

}
