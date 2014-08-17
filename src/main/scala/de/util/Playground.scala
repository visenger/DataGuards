package de.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by visenger on 13/08/14.
 */
case class Customer(custKey: String, name: String, addr: String, natKey: String, phone: String, acc: String, mrkt: String, comment: String)

object Playground {

  def main(args: Array[String]) {

    val config: Config = ConfigFactory.load()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SPARK")
    val sc: SparkContext = new SparkContext(sparkConf)

    val customers = sc.textFile(config.getString("data.tpch.path")).map(line => {
      val Array(custKey, name, addr, natKey, phone, acc, mrkt, comment) = line.split('|')
      Customer(custKey.toString, name.toString, addr.toString, natKey.toString, phone.toString, acc.toString, mrkt.toString, comment.toString)
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD
    customers.registerAsTable("customers")

    val automobile = sqlContext.sql("SELECT custKey, name FROM customers WHERE mrkt='AUTOMOBILE'")

    println("automobile count = " + automobile.count())

    //    automobile.map(m => s"cust key: ${m(0)} name: ${m(1)}").foreach(println)

    import sqlContext._

    val anotherAuto = customers.where('mrkt === "AUTOMOBILE").select('name)
    println("another auto count = " + anotherAuto.count())

    sc.stop()

  }

}


