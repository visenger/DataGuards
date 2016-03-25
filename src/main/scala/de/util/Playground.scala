package de.util

import com.google.common.base.Strings
import com.rockymadden.stringmetric.StringMetric
import com.rockymadden.stringmetric.similarity.{RatcliffObershelpMetric, JaroMetric}
import com.typesafe.config.{Config, ConfigFactory}
import de.data.preparation.{Papers, PaperAuthorAffil, Hosp2Tuple, HospTuple}
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DataType, StructField, StructType}
import org.apache.spark.sql.{Row, GroupedData, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._


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
    import sqlContext.implicits._
    customers.toDF().registerTempTable("customers")
    orders.toDF().registerTempTable("orders")

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
  var i = 1
  tuples.foreach(t => {

    println(s"$i $t")
    i += 1
  })
}

object MsagData {
  val normNames1 = Seq(
    s"department of computer engineering ege university 35100 bornova izmir turkey",
    s"ege universitesi",
    s"ege university",
    s"ege university department of computer engineering")

  val normNames2 = Seq(
    s"chinese academy of sciences",
    s"national institute of neurological disorders and stroke",
    s"capital medical university",
    s"columbia university",
    s"nanjing medical university",
    s"tongji university",
    s"yale university school of medicine",
    s"1synaptic transmission section national institute of neurological disorders and stroke bethesda maryland 20892 and",
    s"fudan university",
    s"nih",
    s"school of life science and technology tongji university shanghai china",
    s"second military medical university",
    s"shanghai center for systems biomedicine laboratory of systems biomedicine of ministry of education shanghai jiao tong university",
    s"tongji university shanghai"
  )

  val originNames = Seq(s"Ege·Üniversitesi",
    s"Ege·University_·Department·of·Computer·Engineering_·35100·Bornova_·Izmir_·Turkey|||Ege·University_·Department·of·Computer·Engineering_·35100·Bornova_·Izmir_·Turkey",
    s"Ege·University|Department·of·Computer·Engineering|||Ege·University|Department·of·Computer·Engineering|||Ege·University·Department·of·Computer·Engineering·Bornova·35100·Izmir·Turkey|||Ege·Uni",
    s"Ege·University|Department·of·Computer·Engineering|||Ege·University_·Department·of·Computer·Engineering_·35100·Bornova_·Izmir_·Turkey|||Ege·University_·Department·of·Computer·Engineering_·351",
    s"Ege·University|||CEA",
    s"Ege·University|||CEA·LIST",
    s"Ege·University_·Computer·Engineering·Department_·Universite·cad.·35100_·Bornova_·Izmir_·Turkey",
    s"Ege·University",
    s"Department·of·Computer·Engineering|Ege·University|Universita·di·Bologna",
    s"Department·of·Computer·Engineering_·Ege·University_·35100·Bornova_·Izmir_·Turkey|||Department·of·Computer·Engineering_·Ege·University_·35100·Bornova_·Izmir_·Turkey|||Department·of·Computer·E",
    s"Ege·University_·Department·of·Computer·Engineering_·35100·Bornova_·Izmir·Turkey|||Ege·University_·Department·of·Computer·Engineering_·35100·Bornova_·Izmir·Turkey",
    s"Department·of·Computer·Engineering_·Ege·University_·35100·Bornova_·Izmir_·Turkey|||Department·of·Computer·Engineering_·Ege·University_·35100·Bornova_·Izmir_·Turkey|||Ege·University|Department")

}

object HospData {

  val telNumbers = Seq("2562358900", "2562358900typo")

  val stateAvg = Seq(
    s"AL_AMI-2",
    s"AL_AMI-3",
    s"AL_AMI-4",
    s"AL_AMI-5",
    s"AL_AMI-7A",
    s"AL_AMI-8Atypo",
    s"AL_CAC-1",
    s"AL_CAC-2",
    s"AL_CAC-3",
    s"AL_HF-1",
    s"AL_HF-2",
    s"AL_HF-3",
    s"AL_HF-4"
  )

  val conditionTopValues = Seq(
    s"Pneumonia",
    s"Surgical Infection Prevention",
    s"Heart Attack",
    s"Heart Failure",
    s"Heart Attacktypo",
    s"Children&#8217;s Asthma Care",
    s"Surgical Infection Preventiontypo",
    s"Children&#8217;s Asthma Caretypo"
  )

  val measureNameTopValues = Seq(

    s"Children Who Received Reliever Medication While Hospitalized for Asthma",
    s"Children Who Received Systemic Corticosteroid Medication (oral and IV Medication That Reduces Inflammation and Controls Symptoms) While Hospitalized for Asthma",
    s"Children and their Caregivers Who Received a Home Management Plan of Care Document While Hospitalized for Asthmatypo",
    s"Heart Attack Patients Given ACE Inhibitor or ARB for Left Ventricular Systolic Dysfunction (LVSD)",
    s"Heart Attack Patients Given Aspirin at Arrival",
    s"Heart Attack Patients Given Aspirin at Discharge",
    s"Heart Attack Patients Given Beta Blocker at Discharge",
    s"Heart Attack Patients Given Fibrinolytic Medication Within 30 Minutes Of Arrival",
    s"Heart Attack Patients Given PCI Within 90 Minutes Of Arrival",
    s"Heart Attack Patients Given Smoking Cessation Advice/Counseling",
    s"Heart Failure Patients Given ACE Inhibitor or ARB for Left Ventricular Systolic Dysfunction (LVSD)",
    s"Heart Failure Patients Given Discharge Instructions",
    s"Heart Failure Patients Given Smoking Cessation Advice/Counseling",
    s"Heart Failure Patients Given an Evaluation of Left Ventricular Systolic (LVS) Function"

  )
}

object ApproximateMatchTester extends App {

  //  private val compare: Option[Double] = JaroMetric.compare("dwayne", "duane")
  //  println(compare.get)

  val combinations: List[(String, String)] = getPairs(MsagData.normNames1)

  computeSimilarities(combinations)

  def computeSimilarities(pairs: List[(String, String)]): List[String] = {
    val default: Double = 0.0

    val header = s"datapointX, datapointY:, jaccard:, jaroWinkler:, levenstein:, diceSorensen:, ratcliffOber:, overlap:"
    println(header)
    val similarities: List[String] = for ((x, y) <- pairs) yield {
      val jaccard: Double = StringMetric.compareWithJaccard(1)(x.toCharArray, y.toCharArray).getOrElse(default)
      val overlap: Double = StringMetric.compareWithOverlap(3)(x.toCharArray, y.toCharArray).getOrElse(default)
      val jaroWinkler: Double = StringMetric.compareWithJaroWinkler(x.toCharArray, y.toCharArray).getOrElse(default)
      val diceSorensen: Double = StringMetric.compareWithDiceSorensen(1)(x.toCharArray, y.toCharArray).getOrElse(default)
      val levenstein: Int = StringMetric.compareWithLevenshtein(x.toCharArray, y.toCharArray).getOrElse(0)
      val ratcliffOber: Double = RatcliffObershelpMetric.compare(x.toCharArray, y.toCharArray).getOrElse(default)

      val row: String = s"[$x],[$y],${jaccard},${jaroWinkler},${levenstein},${diceSorensen},${ratcliffOber},${overlap}"
      // println(row)
      row
    }
    similarities
  }


  def getPairs(in: Seq[String]): List[(String, String)] = {
    in.combinations(2).map(x => (x.head, x.tail.head)).toList
  }
}


//todo: return Seq of Values -> data types supported by spark
//package object df_utils {
//  val default: Double = 0.0
//
//  implicit class TuplesExtension(val pairs: List[(String, String)]) {
//
//    def computeSimilarities: List[String] = {
//      val header = s"datapointX, datapointY:, jaccard:, jaroWinkler:, levenstein:, diceSorensen:, ratcliffOber:, overlap:"
//      //println(header)
//      val similarities: List[String] = for ((x, y) <- pairs) yield {
//        val jaccard: Double = compareWithJaccard(x, y)
//        val overlap: Double = compareWithOverlap(x, y)
//        val jaroWinkler: Double = compareWithJaroWinkler(x, y)
//        val diceSorensen: Double = compareWithDiceSorensen(x, y)
//        val levenstein: Int = getLevensteinDistance(x, y)
//        val ratcliffOber: Double = compareWithRatcliffObershelp(x, y)
//
//        val row: String = s"[$x],[$y],${jaccard},${jaroWinkler},${levenstein},${diceSorensen},${ratcliffOber},${overlap}"
//        // println(row)
//        row
//      }
//      header :: similarities
//    }
//
//    private def compareWithRatcliffObershelp(x: String, y: String): Double = {
//      RatcliffObershelpMetric.compare(x.toCharArray, y.toCharArray).getOrElse(default)
//    }
//
//    private def getLevensteinDistance(x: String, y: String): Int = {
//      val notApplicable: Int = Int.MaxValue
//      StringMetric.compareWithLevenshtein(x.toCharArray, y.toCharArray).getOrElse(notApplicable)
//    }
//
//    private def compareWithDiceSorensen(x: String, y: String): Double = {
//      StringMetric.compareWithDiceSorensen(1)(x.toCharArray, y.toCharArray).getOrElse(default)
//    }
//
//    private def compareWithJaroWinkler(x: String, y: String): Double = {
//      StringMetric.compareWithJaroWinkler(x.toCharArray, y.toCharArray).getOrElse(default)
//    }
//
//    private def compareWithOverlap(x: String, y: String): Double = {
//      val nGram: Int = 3
//      StringMetric.compareWithOverlap(nGram)(x.toCharArray, y.toCharArray).getOrElse(default)
//    }
//
//    private def compareWithJaccard(x: String, y: String): Double = {
//      StringMetric.compareWithJaccard(1)(x.toCharArray, y.toCharArray).getOrElse(default)
//    }
//
//
//  }
//
//  implicit class DFExtension(val df: DataFrame) {
//    def topK(colName: String, k: Int): DataFrame = {
//      df.
//        select(colName).
//        filter(df(colName) !== "").
//        groupBy(colName).
//        count().
//        orderBy(desc("count")).limit(k)
//    }
//
//    def top10(colName: String, k: Int = 10): DataFrame = {
//      df.
//        select(colName).
//        filter(df(colName) !== "").
//        groupBy(colName).
//        count().
//        orderBy(desc("count")).limit(k)
//    }
//
//    def valueCombiByColumn(colName: String): List[(String, String)] = {
//      val rdd: RDD[Row] = df.select(colName).rdd
//      val valuesAsStr: List[String] = rdd.map(r => r(0).toString).collect().toList
//      val pairs: List[(String, String)] = valuesAsStr.combinations(2).map(x => (x.head, x.tail.head)).toList
//
//      pairs
//    }
//
//
//  }
//
//
//}


//todo 1: analyse columns from profiled FDs.
//todo 2: select non-primary key
//todo 3: for each column and cols combination run simi and correlation profiling
object BasicStatisticsWithSpark extends App {
  val config = ConfigFactory.load()
  val path: String = config.getString("data.msag.path")
  val author = "author19525FF1.txt"
  val papers = "papers19525FF1.txt"
  val conf = new SparkConf().setMaster("local[4]").setAppName("MSAG")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val authorTuples = sc.textFile(s"$path/$author").map(t => {
    val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr) = t.split("\\t")
    val paperAuthorAffil: PaperAuthorAffil = PaperAuthorAffil(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr)
    paperAuthorAffil
  })

  val papersByAuthor = sc.textFile(s"$path/$papers").map(t => {
    val Array(paperId, originTitle, normalTitle, publishYear, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
    val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank)
    paper
  })

  val authorsDF: DataFrame = sqlContext.createDataFrame(authorTuples)
  val papersDF: DataFrame = sqlContext.createDataFrame(papersByAuthor)


  val paperid: String = "paperId"
  val authorid: String = "authorId"
  val affilid: String = "affilId"
  val originaffil: String = "originAffil"
  val normalaffil: String = "normalAffil"
  val publishyear: String = "publishYear"

  val joinedAuthorPaper =
    authorsDF.
      join(papersDF, authorsDF(paperid) === papersDF(paperid)).
      select(authorsDF(paperid), authorsDF(authorid), authorsDF(affilid), authorsDF(originaffil),
        authorsDF(normalaffil), papersDF(publishyear))

  joinedAuthorPaper.show()


  private val selectAffil: DataFrame = joinedAuthorPaper.select(affilid)
  val countAffiliations: Long = selectAffil.count()
  println(s"count affils from joint tables $countAffiliations")

  val distinctAffiliation: DataFrame = selectAffil.distinct()
  val countDistinctAffils: Long = distinctAffiliation.count()
  println(s"count distinct affils $countDistinctAffils")

  import de.data.dataframe_util._

  val countAffilIDs: DataFrame = joinedAuthorPaper.top10(affilid)


  countAffilIDs.show()
  private val topK: DataFrame = joinedAuthorPaper.topK(affilid, k = 5)
  topK.show()

  println(topK.queryExecution.simpleString)

  val topKAsList: List[Any] = topK.select(affilid).rdd.map(r => r(0)).collect().toList

  topKAsList foreach (println)

  val affiliations: DataFrame = joinedAuthorPaper.select(normalaffil)

  val combiByColumn: List[String] = affiliations.top10(normalaffil).stringValuesByColumn(normalaffil)

  val similarities: List[String] = combiByColumn.computeSimilaritiesOnColumn

  similarities foreach (println)


  /*
  *
  *
+--------+-----+
| affilId|count|
+--------+-----+
|012E6F4E|   10|
|02BA878E|    4|
|04FA6952|    3|
|06C38422|    2|
|04AF010A|    2|
|06F892B4|    2|
|09C9BF4E|    2|
|02CC7132|    1|
|0B5FC046|    1|
|02CC4D78|    1|
|164CE32B|    1|
|0709D66D|    1|
|17E5897D|    1|
|0A9B0CA5|    1|
|017C99DB|    1|
|15927FB4|    1|
+--------+-----+
limit 10

+--------+-----+
| affilId|count|
+--------+-----+
|012E6F4E|   10|
|02BA878E|    4|
|04FA6952|    3|
|04AF010A|    2|
|06C38422|    2|
|06F892B4|    2|
|09C9BF4E|    2|
|0709D66D|    1|
|02CC4D78|    1|
|17E5897D|    1|
+--------+-----+

  *
  * */


  /*authorsDF.show(3)

  authorsDF.printSchema()

  private val distinctVals: DataFrame = authorsDF.select(normalaffil).distinct()

  distinctVals.show()

  private val count: Long = distinctVals.count()
  println(s"number of distinct values in $count")*/

  // IllegalArgumentException: requirement failed: Covariance calculation for columns with dataType StringType not supported.
  //  private val corr: Double = authorsDF.select("affilId").stat.corr("affilId", "paperId")
  //  println(s"correlation $corr")

  //todo: run mllib statistics here:
  //http://spark.apache.org/docs/latest/mllib-statistics.html

  sc.stop()

}

//todo: schema inference ...
// should be similar to this:
// https://github.com/databricks/spark-csv/blob/master/src/main/scala/com/databricks/spark/csv/util/InferSchema.scala
object SchemaInferer extends App {
  val config = ConfigFactory.load()
  val path: String = config.getString("data.msag.path")
  val author = "author19525FF1.txt"
  val papers = "papers19525FF1.txt"
  val conf = new SparkConf().setMaster("local[4]").setAppName("MSAG")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val rows: RDD[Row] = sc.textFile(s"$path/$author").map(t => {
    val generalSplit: Array[String] = t.split("\\t")
    val row: Row = DataTuple(generalSplit: _*).createRow()
    row
  })


  val structType: StructType = DataTuple.createStructType(rows.first())
  val generalDF: DataFrame = sqlContext.createDataFrame(rows, structType)

  generalDF.show(10)

  generalDF.printSchema()


  /* val papersByAuthor = sc.textFile(s"$path/$papers").map(t => {
     val Array(paperId, originTitle, normalTitle, publishYear, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
     val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank)
     paper
   })


   val papersDF: DataFrame = sqlContext.createDataFrame(papersByAuthor)*/
}

case class DataTuple(attr: String*) {
  def createRow(): Row = {
    Row.fromSeq(attr)
  }


}

object DataTuple {
  def createStructType(attributes: Row): StructType = {

    var count = 0

    val structType: StructType = attributes.toSeq.foldLeft(StructType(Nil))(
      (accamulator, element) => {
        accamulator.add({
          count += 1;
          s"column$count"
        }, DataTypes.StringType, true)
      }
    )
    structType
  }
}


