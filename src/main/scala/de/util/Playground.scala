package de.util

import com.rockymadden.stringmetric.StringMetric
import com.rockymadden.stringmetric.similarity.JaroMetric
import com.typesafe.config.{Config, ConfigFactory}
import de.data.preparation.{Hosp2Tuple, HospTuple}
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
    customers.registerTempTable("customers")
    orders.registerTempTable("orders")

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

    println("config.getString " + config.getString("data.tpch.orders"))
    val jointCustOrder: RDD[JointCustOrder] = jointTables.map(m =>
      JointCustOrder(s"${m(0)}", s"${m(1)}", s"${m(2)}", s"${m(3)}", s"${m(4)}", s"${m(5)}", s"${m(6)}", s"${m(7)}", s"${m(8)}", s"${m(9)}", s"${m(10)}", s"${m(11)}", s"${m(12)}"))


    println("jointTables count = " + jointCustOrder.count())

    //jointTables.take(25).map(m => s"cust key: ${m(0)} cust name: ${m(1)} order key: ${m(2)}").foreach(println)
    jointCustOrder.take(25).map(m => s"customer: ${m.custKey} name: ${m.name} bought item: ${m.orderKey} for total price: ${m.totalPrice}").foreach(println)

    //    val indexedTabs: RDD[(JointCustOrder, Long)] = jointCustOrder.zipWithUniqueId()
    //    indexedTabs.saveAsTextFile(config.getString("data.tpch.resultFolder"))

    //    import sqlContext._
    //    val anotherAuto = customers.where('mrkt === "AUTOMOBILE").select('name)
    //    println("another auto count = " + anotherAuto.count())

    sc.stop()

  }

}

object TestMatch extends App {
  val line = "10018,CALLAHAN EYE FOUNDATION HOSPITAL,1720 UNIVERSITY BLVD,,,BIRMINGHAM,AL,35233,JEFFERSON,2053258100,Acute Care Hospitals,Voluntary non-profit - Private,Yes,Surgical Infection Prevention,SCIP-CARD-2,surgery patients who were taking heart drugs called beta blockers before coming to the hospital&#54; who were kept on the beta blockers during the period just before and after their surgery,,,AL_SCIP-CARD-2"
  val Array(providerID, x1, x2, x3, x4, city, state, zipCode, x5, phoneNumber, x6, x7, x8, condition, measureID, measureName, x9, x10, stateAvg) = line.split(',')

  val item = Hosp2Tuple(providerID.toString.replace('"', ' ').trim, city.toString, state.toString, zipCode.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, stateAvg.toString)

  println("hosp2 = " + item.toString)

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

object HospStat extends App {
  val config = ConfigFactory.load()


  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
  val sc: SparkContext = new SparkContext(sparkConf)

  //val header: String = sc.textFile(datapath).first()

  val filteredHeader: RDD[String] = sc.textFile(s"${config.getString("data.hosp.path")}/hosp-1.tsv")
  val strCount: Long = filteredHeader.count()
  //    val filteredHeader: RDD[String] = sc.parallelize(sc.textFile(datapath).filter(!_.equals(header)).take(200))
  val tupled: RDD[HospTuple] = filteredHeader.map(line => {
    val Array(providerID, hospitalName, address, city, state, zipCode, countyName, phoneNumber, condition, measureID, measureName, score, sample, footnote, measureStartDate, measureEndDate) = line.split( s"""","""")
    HospTuple(providerID.toString.replace('"', ' ').trim, hospitalName.toString, address.toString, city.toString, state.toString, zipCode.toString, countyName.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, score.toString, sample.toString, footnote.toString, measureStartDate.toString, measureEndDate.toString)
  })

  val count = tupled.count()

  val groupedById: RDD[(String, scala.Iterable[HospTuple])] = tupled.groupBy(_.providerID)
  val data: List[(String, Iterable[HospTuple])] = groupedById.collect().toList
  sc.stop


  data foreach (g => {
    println(s"${g._1} : ${g._2.size} : % ${(g._2.size.toDouble / count.toDouble) * 100.0} : count= $strCount")
  })


}

object CombinationsTester extends App {

  implicit class Crossable[T](xs: Traversable[T]) {
    def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val list = List('a, 'b, 'c, 'd)
  val tuples: List[((Symbol, Symbol), Symbol)] = (list cross list cross list).toList
  var i = 1;
  tuples.foreach(t => {

    println(s"${i} $t")
    i += 1
  })
}

object ApproximateMatchTester extends App {
  val input = Seq("department of computer engineering ege university 35100 bornova izmir turkey",
    "ege universitesi",
    "ege university",
    "ege university department of computer engineering")
  private val compare: Option[Double] = JaroMetric.compare("dwayne", "duane")
  println(compare.get)

  val combinations: List[(String, String)] = getPairs(input)

  val default: Double = 0.0
  for ((x, y) <- combinations) {
    val jaccard: Double = StringMetric.compareWithJaccard(1)(x.toCharArray, y.toCharArray).getOrElse(default)
    val jaro: Double = StringMetric.compareWithJaro(x.toCharArray, y.toCharArray).getOrElse(default)
    val levenstein: Int = StringMetric.compareWithLevenshtein(x.toCharArray, y.toCharArray).getOrElse(0)
    println(s"[$x] compared to [$y] : jaccard: ${jaccard}, jaro: ${jaro}, levenstein: ${levenstein} ")
  }


  def getPairs(in: Seq[String]): List[(String, String)] = {
    in.combinations(2).map(x => (x.head, x.tail.head)).toList
  }
}


