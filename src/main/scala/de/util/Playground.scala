package de.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.util.Random

/**
 * Created by visenger on 13/08/14.
 */


case class Customer(custKey: String, name: String, addr: String, natKey: String, phone: String, acc: String, mrkt: String, comment: String)

case class Order(orderKey: String, custKey: String, orderStatus: String, totalPrice: String, orderDate: String, orderPriority: String, clerk: String, shipPriority: String, comment: String)

case class JointCustOrder(custKey: String, name: String, addr: String, natKey: String, phone: String, acc: String, mrkt: String, orderKey: String, orderStatus: String, totalPrice: String, orderDate: String, orderPriority: String, clerk: String)


object Playground {

  def main(args: Array[String]) {
    implicit class Crossable[T](xs: Traversable[T]) {
      def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
    }

    val config: Config = ConfigFactory.load()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SPARK")
    val sc: SparkContext = new SparkContext(sparkConf)

    val orders: RDD[Order] = sc.textFile(config.getString("data.tpch.orders")).map(line => {
      val Array(orderKey, custKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority, comment) = line.split('|')
      Order(orderKey.toString, custKey.toString, orderStatus.toString, totalPrice.toString, orderDate.toString, orderPriority.toString, clerk.toString, shipPriority.toString, comment.toString)
    })

    val customers = sc.textFile(config.getString("data.tpch.customers")).map(line => {
      val Array(custKey, name, addr, natKey, phone, acc, mrkt, comment) = line.split('|')
      Customer(custKey.toString, name.toString, addr.toString, natKey.toString, phone.toString, acc.toString, mrkt.toString, comment.toString)
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD
    customers.registerAsTable("customers")
    orders.registerAsTable("orders")

    val jointTables = sqlContext.sql("SELECT c.custKey, c.name, c.addr, c.natKey, c.phone, c.acc, c.mrkt,  " +
      "o.orderKey, o.orderStatus, o.totalPrice, o.orderDate, o.orderPriority, o.clerk " +
      "FROM customers c, orders o " +
      "WHERE c.custKey=o.custKey")
    /*
    * 0 c.custKey,
    * 1 c.name,
    * 2 c.addr,
    * 3 c.natKey,
    * 4 c.phone,
    * 5 c.acc,
    * 6 c.mrkt,
    * 7 o.orderKey,
    * 8 o.orderStatus,
    * 9 o.totalPrice,
    * 10 o.orderDate,
    * 11 o.orderPriority,
    * 12 o.clerk
    *
    *
    * */

    val jointCustOrder: RDD[JointCustOrder] = jointTables.map(m =>
      JointCustOrder(s"${m(0)}", s"${m(1)}", s"${m(2)}", s"${m(3)}", s"${m(4)}", s"${m(5)}", s"${m(6)}", s"${m(7)}", s"${m(8)}", s"${m(9)}", s"${m(10)}", s"${m(11)}", s"${m(12)}"))


    println("jointTables count = " + jointCustOrder.count())

    //jointTables.take(25).map(m => s"cust key: ${m(0)} cust name: ${m(1)} order key: ${m(2)}").foreach(println)
    jointCustOrder.take(25).map(m => s"customer: ${m.custKey} name: ${m.name} bought item: ${m.orderKey} for total price: ${m.totalPrice}").foreach(println)

    val indexedTabs: RDD[(JointCustOrder, Long)] = jointCustOrder.zipWithUniqueId()
    indexedTabs.saveAsTextFile(config.getString("data.tpch.resultFolder"))

    //    import sqlContext._
    //    val anotherAuto = customers.where('mrkt === "AUTOMOBILE").select('name)
    //    println("another auto count = " + anotherAuto.count())

    sc.stop()

  }

}

object TestMatch extends App {
  val line = "579|67057|O|161591.06|1998-03-11|2-HIGH|Clerk#000000862|0| regular instructions. blithely even p|"
  val Array(orderKey, custKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority, comment) = line.split('|')
  val order = Order(orderKey.toString, custKey.toString, orderStatus.toString, totalPrice.toString, orderDate.toString, orderPriority.toString, clerk.toString, shipPriority.toString, comment.toString)
  println("order = " + order.toString)

}


object TestMe extends App {

  implicit class Crossable[T](xs: Traversable[T]) {
    def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val noise = 5

  val num = /*220986*/ 35 * 16 * noise / 100

  println(s" noise elements with $noise%  = " + num)


  val attrs = (2 to 16).toList
  val tuples = (0 to 34).toList

  val c = tuples cross attrs

  println("c.size = " + c.size)

  val noisy = Random.shuffle(c).take(num)
  println("noisy = " + noisy)


}

object TestSubstring extends App {
  val str = "hosp.tsv"

  val substr: String = str.takeWhile(_ != '.')

  println("substr = " + substr)
}

object TestConverter extends App {
  val str = "9231\t9"
  val list: List[String] = str.split("\\t").toList

  val a: Long = convertToLong(list.head)
  println("a = " + a)

  val t: List[Int] = convertToInt(list.tail)
  println("t = " + t)


  def convertToLong(s: String): Long = {
    s.trim.toLong
  }

  def convertToInt(l: List[String]): List[Int] = {
    l.map(_.trim.toInt)
  }
}


