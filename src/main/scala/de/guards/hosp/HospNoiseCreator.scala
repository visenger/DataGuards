package de.guards.hosp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by visenger on 17/08/14.
 */

case class HospTuple(providerID: String, hospitalName: String, address: String, city: String, state: String, zipCode: String, countyName: String, phoneNumber: String, condition: String, measureID: String, measureName: String, score: String, sample: String, footnote: String, measureStartDate: String, measureEndDate: String)

object HospNoiseCreator extends App {

  val conf: Config = ConfigFactory.load()

  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
  val sc: SparkContext = new SparkContext(sparkConf)

  val path: String = conf.getString("data.hosp.path")
  private val firstLine: String = sc.textFile(path).first()
  println("firstLine = " + firstLine)
  val allTuples /*: RDD[HospTuple]*/ = sc.textFile(path).filter(!_.equals(firstLine)).map(line => {

    val Array(providerID, hospitalName, address, city, state, zipCode, countyName, phoneNumber, condition, measureID, measureName, score, sample, footnote, measureStartDate, measureEndDate) = line.split( s"""","""")
    HospTuple(providerID.toString, hospitalName.toString, address.toString, city.toString, state.toString, zipCode.toString, countyName.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, score.toString, sample.toString, footnote.toString, measureStartDate.toString, measureEndDate.toString)

  })

  println("allTuples = " + allTuples.count())

  sc.stop()
}

object TestingHosp extends App {

  val str = s""""010007","MIZELL MEMORIAL HOSPITAL","702 N MAIN ST","OPP","AL","36467","COVINGTON","3344933541","Blood Clot Prevention and Treatment","VTE_6","Incidence of potentially preventable VTE","Not Available","0","2 - Data submitted were based on a sample of cases/patients., 7 - No cases met the criteria for this measure.","01/01/2013","09/30/2013""""

  val Array(providerID, hospitalName, address, city, state, zipCode, countyName, phoneNumber, condition, measureID, measureName, score, sample, footnote, measureStartDate, measureEndDate) = str.split( s"""","""")

  println("providerID = " + providerID)
  println("countyName = " + countyName)
  println("measureStartDate = " + measureStartDate)
}
