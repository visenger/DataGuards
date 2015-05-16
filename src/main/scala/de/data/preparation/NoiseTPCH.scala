package de.data.preparation

import com.typesafe.config.{ConfigFactory, Config}
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.Iterable
import scala.util.Random

/**
 * Processing TPCH data set. This class introduces noise to TPCH. Result is written to the specified folder.
 */
/*The reason for extending the class with Serializable
http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when*/

class TpchNoiseInjector(val datapath: String, val noisePercentage: Int = 2, val writeTo: String) extends java.io.Serializable {

  implicit class Crossable[T](xs: Traversable[T]) {
    def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val config = ConfigFactory.load()

  def inject = {
    for {setSize <- Array(500/*, 1000, 10000, 20000, 30000, 40000, 50000, 70000, 90000, 100000*/)} {
      println(s"input = $datapath; noise = $noisePercentage; result folder= $writeTo")

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

      //todo: split jointTables into smaller parts, which will be suitable for Rockit inference.


      val tablePart: RDD[Row] = sc.parallelize(Random.shuffle(jointTables).take(setSize))

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
      * */


      val jointCustOrder: RDD[JointCustOrder] = tablePart.map(m =>
        JointCustOrder(s"${m(0)}", s"${m(1)}", s"${m(2)}", s"${m(3)}", s"${m(4)}", s"${m(5)}", s"${m(6)}", s"${m(7)}", s"${m(8)}", s"${m(9)}", s"${m(10)}", s"${m(11)}", s"${m(12)}"))

      val indexedTabs: RDD[(Long, JointCustOrder)] = jointCustOrder.zipWithUniqueId().map(e => e.swap)

      val count: Long = indexedTabs.count()

      val noiseElements: List[(Long, Int)] = calculateNoise(count)
      val groupedByKey: Map[Long, List[(Long, Int)]] = noiseElements.groupBy(_._1)

      val compactView: Map[Long, List[Int]] = groupedByKey.map(t => {
        val attrsList: List[Int] = t._2.map(l => l._2)
        (t._1, attrsList)
      })

      val dirtyData: RDD[(Long, JointCustOrder)] = indexedTabs.map(t => {
        if (compactView.contains(t._1))
          (t._1, insertNoiseInto(t._2, compactView.getOrElse(t._1, List())))
        else (t._1, t._2)
      })

      val markovLogicPredicates: RDD[String] = dirtyData.map(t => {
        t._2.createPredicates(t._1)
      })

      //todo: folder namings
      val outputFolder: String = s"$writeTo/$noisePercentage/$setSize"
      markovLogicPredicates.saveAsTextFile(outputFolder)

      sc.stop()

      val logData: Iterable[String] = compactView.map(t => {
        s"""${t._1.toString}\t${t._2.mkString("\t")}"""
      })

      Util.writeToFile(logData.toList, s"$outputFolder/log-dataSize-$setSize-noise-$noisePercentage.tsv")
    }
  }


  //function intstead of method due to SparkException: Job aborted due to stage failure:
  // Task not serializable: java.io.NotSerializableException
  val insertNoiseInto: (JointCustOrder, List[Int]) => JointCustOrder = (tuple, attrs) => {
    tuple.insertNoise(attrs)

  }

  val addThenDouble: (Int, Int) => Int = (x, y) => {
    val a = x + y
    2 * a
  }


  val calculateNoise: (Long) => List[(Long, Int)] = (count: Long) => {
    val attrCount = config.getInt("data.tpch.attrCount")
    val totalElementsCount: Long = count * attrCount
    val noisyElementsCount: Long = totalElementsCount * noisePercentage / 100

    val attrsIdx = (2 to attrCount).toList
    val tuplesIdx = (0.toLong to count).toList

    val matrix: List[(Long, Int)] = tuplesIdx.cross(attrsIdx).toList

    val noisyElements: List[(Long, Int)] = Random.shuffle(matrix).take(noisyElementsCount.toInt)

    noisyElements
  }

}

// bean classes.

case class Customer(custKey: String, name: String, addr: String, natKey: String, phone: String, acc: String, mrkt: String, comment: String)

case class Order(orderKey: String, custKey: String, orderStatus: String, totalPrice: String, orderDate: String, orderPriority: String, clerk: String, shipPriority: String, comment: String)

case class JointCustOrder(var custKey: String,
                          var name: String,
                          var addr: String,
                          var natKey: String,
                          var phone: String,
                          var acc: String,
                          var mrkt: String,
                          var orderKey: String,
                          var orderStatus: String,
                          var totalPrice: String,
                          var orderDate: String,
                          var orderPriority: String,
                          var clerk: String) {


  def insertNoiseToname(str: String) = {
    this.name = this.name + str
  }

  def insertNoiseToaddr(str: String) = {
    this.addr = this.addr + str
  }

  def insertNoiseTonatKey(str: String) = {
    this.natKey = this.natKey + str
  }

  def insertNoiseTophone(str: String) = {
    this.phone = this.phone + str
  }

  def insertNoiseToacc(str: String) = {
    this.acc = this.acc + str
  }

  def insertNoiseTomrkt(str: String) = {
    this.mrkt = this.mrkt + str
  }


  val insertNoise: (List[Int]) => this.type = (attrs) => {

    val noise = "typo"
    attrs.foreach(i => i match {
      case 2 => insertNoiseToname(noise)
      case 3 => insertNoiseToaddr(noise)
      case 4 => insertNoiseTonatKey(noise)
      case 5 => insertNoiseTophone(noise)
      case 6 => insertNoiseToacc(noise)
      case 7 => insertNoiseTomrkt(noise)
      case _ =>
    })
    this
  }

  val createPredicates: (Long) => String = (idx) => {
    import Util._
//        s"""custKey("$idx", "${normalizeGroundAtom(this.custKey)}")
//           |name("$idx", "${normalizeGroundAtom(this.name)}")
//           |addr("$idx", "${normalizeGroundAtom(this.addr)}")
//           |natKey("$idx", "${normalizeGroundAtom(this.natKey)}")
//           |phone("$idx", "${normalizeGroundAtom(this.phone)}")
//           |acc("$idx", "${normalizeGroundAtom(this.acc)}")
//           |mrkt("$idx", "${normalizeGroundAtom(this.mrkt)}")
//           |orderKey("$idx", "${normalizeGroundAtom(this.orderKey)}")
//           |orderStatus("$idx", "${normalizeGroundAtom(this.orderStatus)}")
//           |totalPrice("$idx", "${normalizeGroundAtom(this.totalPrice)}")
//           |orderDate("$idx", "${normalizeGroundAtom(this.orderDate)}")
//           |orderPriority("$idx", "${normalizeGroundAtom(this.orderPriority)}")
//           |clerk("$idx", "${normalizeGroundAtom(this.clerk)}")
//         """.stripMargin

    s"""custKey("$idx", "${normalizeGroundAtom(this.custKey)}")
        |name("$idx", "${normalizeGroundAtom(this.name)}")
        |addr("$idx", "${normalizeGroundAtom(this.addr)}")
        |natKey("$idx", "${normalizeGroundAtom(this.natKey)}")
        |phone("$idx", "${normalizeGroundAtom(this.phone)}")
        |acc("$idx", "${normalizeGroundAtom(this.acc)}")
        |mrkt("$idx", "${normalizeGroundAtom(this.mrkt)}")
        |orderKey("$idx", "${normalizeGroundAtom(this.orderKey)}")
     """.stripMargin
  }

  val dictionary: String =
  s"""name \t 2
     |addr \t 3
     |natKey \t 4
     |phone \t 5
     |acc \t 6
     |mrkt \t 7
   """.stripMargin


}

object TPCHTuple {
  def getIdxByAttrName(attr: String): Int = {


    attr match {
      case "eqNames" => 2
      case "eqAddr" => 3
      case "eqNatkey" => 4
      case "eqPhone" => 5
      case "eqAcc" => 6
      case "eqMrkt" => 7
      case _ => Int.MinValue

    }


  }
}

