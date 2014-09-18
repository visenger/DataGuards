package de.data.preparation

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Introduces noise to HOSP data set and writes results to the specified folder.
 */


class HospNoiseInjector(val datapath: String, val noisePercentage: Int = 2, val writeTo: String) {

  implicit class Crossable[T](xs: Traversable[T]) {
    def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val config = ConfigFactory.load()


  def readData: Map[Long, HospTuple] = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
    val sc: SparkContext = new SparkContext(sparkConf)

    val header: String = sc.textFile(datapath).first()

    val filteredHeader: RDD[String] = sc.textFile(datapath).filter(!_.equals(header))
    val tupled: RDD[HospTuple] = filteredHeader.map(line => {
      val Array(providerID, hospitalName, address, city, state, zipCode, countyName, phoneNumber, condition, measureID, measureName, score, sample, footnote, measureStartDate, measureEndDate) = line.split( s"""","""")
      HospTuple(providerID.toString.replace('"', ' ').trim, hospitalName.toString, address.toString, city.toString, state.toString, zipCode.toString, countyName.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, score.toString, sample.toString, footnote.toString, measureStartDate.toString, measureEndDate.toString)
    })

    val tuplesWithId: RDD[(Long, HospTuple)] = tupled.zipWithUniqueId().map(_.swap)
    val tuples: Map[Long, HospTuple] = tuplesWithId.collect().toMap
    sc.stop

    tuples
  }

  def calculateNoiseElements(size: Int): List[(Long, Int)] = {
    val attrCount = config.getInt("data.hosp.attrCount")
    val totalElementsCount: Int = size * attrCount
    val noisyElementsCount: Int = totalElementsCount * noisePercentage / 100

    val attrsIdx = (2 to attrCount).toList
    val tuplesIdx = (0.toLong to size.toLong).toList

    val matrix: List[(Long, Int)] = tuplesIdx.cross(attrsIdx).toList

    val noisyElements: List[(Long, Int)] = Random.shuffle(matrix).take(noisyElementsCount)

    noisyElements
  }

  def insertNoise(input: Map[Long, HospTuple], noiseIdx: List[(Long, Int)]): Map[Long, HospTuple] = {

    val groupedByTupleIdx = noiseIdx.groupBy(_._1)

    val outputWithNoise: Map[Long, HospTuple] = input.map(i => {
      if (groupedByTupleIdx.contains(i._1)) (i._1, insertNoiseInto(i._2, groupedByTupleIdx.getOrElse(i._1, List()))) else (i._1, i._2)
    })
    outputWithNoise
  }

  def inject = {
    println(s"input = $datapath; noise = $noisePercentage; result folder= $writeTo")

    val input: Map[Long, HospTuple] = readData
    val noiseElements: List[(Long, Int)] = calculateNoiseElements(input.size)
    val output: Map[Long, HospTuple] = insertNoise(input, noiseElements)
    println("output.size = " + output.size)
    //todo: persist noisy elements and noisy output into one folder according to the percentage
  }

  private def insertNoiseInto(tuple: HospTuple, idx: List[(Long, Int)]): HospTuple = {
    val attrs: List[Int] = idx.map(_._2)
    val noise: HospTuple = tuple.insertNoise(attrs)
    noise
  }


}


case class HospTuple(providerID: String,
                     var hospitalName: String,
                     var address: String,
                     var city: String,
                     var state: String,
                     var zipCode: String,
                     var countyName: String,
                     var phoneNumber: String,
                     var condition: String,
                     var measureID: String,
                     var measureName: String,
                     var score: String,
                     var sample: String,
                     var footnote: String,
                     var measureStartDate: String,
                     var measureEndDate: String) {

  def insertNoise(attrs: List[Int]): this.type = {
    val noise = "typo"
    attrs foreach (i => i match {

      case 2 => this.hospitalName + noise
      case 3 => this.address + noise
      case 4 => this.city + noise
      case 5 => this.state + noise
      case 6 => this.zipCode + noise
      case 7 => this.countyName + noise
      case 8 => this.phoneNumber + noise
      case 9 => this.condition + noise
      case 10 => this.measureID + noise
      case 11 => this.measureName + noise
      case 12 => this.score + noise
      case 13 => this.sample + noise
      case 14 => this.footnote + noise
      case 15 => this.measureStartDate + noise
      case 16 => this.measureEndDate + noise
    })
    this
  }
}
