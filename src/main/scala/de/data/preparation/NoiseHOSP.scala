package de.data.preparation

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Introduces noise to HOSP data set and writes results to the specified folder.
 */


class HospNoiseInjector(val datapath: String, val noisePersentage: Int = 2, val writeTo: String) {

  val config = ConfigFactory.load()

  def inject = {
    println(s"input = $datapath; noise = $noisePersentage; result folder= $writeTo")
  }

  def readData: Map[Long, HospTuple] = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
    val sc: SparkContext = new SparkContext(sparkConf)

    val path: String = config.getString("data.hosp.path")
    val header: String = sc.textFile(path).first()

    val filteredHeader: RDD[String] = sc.textFile(path).filter(!_.equals(header))
    val tupled: RDD[HospTuple] = filteredHeader.map(line => {
      val Array(providerID, hospitalName, address, city, state, zipCode, countyName, phoneNumber, condition, measureID, measureName, score, sample, footnote, measureStartDate, measureEndDate) = line.split( s"""","""")
      HospTuple(providerID.toString.replace('"', ' ').trim, hospitalName.toString, address.toString, city.toString, state.toString, zipCode.toString, countyName.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, score.toString, sample.toString, footnote.toString, measureStartDate.toString, measureEndDate.toString)
    })

    val tuplesWithId: RDD[(Long, HospTuple)] = tupled.zipWithUniqueId().map(_.swap)
    val tuples: Map[Long, HospTuple] = tuplesWithId.collect().toMap
    sc.stop
    tuples
  }

  def calculateNoiseElements(size: Int): Int = {
    val attrCount = config.getInt("data.hosp.attrCount")
    val totalElementsCount: Int = size * attrCount
    val noisyElementsCount: Int = totalElementsCount * noisePersentage / 100
    noisyElementsCount
  }





  //todo: perform the noise injection according to the introduced parameters;
  //todo: write to file
  //todo: write logs about noise: for evaluation

}



case class HospTuple(providerID: String, hospitalName: String, address: String, city: String, state: String, zipCode: String, countyName: String, phoneNumber: String, condition: String, measureID: String, measureName: String, score: String, sample: String, footnote: String, measureStartDate: String, measureEndDate: String)
