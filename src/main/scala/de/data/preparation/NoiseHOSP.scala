package de.data.preparation

import com.typesafe.config.ConfigFactory
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.Iterable
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
      if (groupedByTupleIdx.contains(i._1)) {
        (i._1, insertNoiseInto(i._2, groupedByTupleIdx.getOrElse(i._1, List())))
      } else (i._1, i._2)
    })
    outputWithNoise
  }

  def inject = {

    val input: Map[Long, HospTuple] = readData
    val noiseElements: List[(Long, Int)] = calculateNoiseElements(input.size)
    val output: Map[Long, HospTuple] = insertNoise(input, noiseElements)

    val logNoise: List[String] = prepareList(noiseElements)
    Util.writeToFile(logNoise, s"$writeTo/$noisePercentage/log-noise-$noisePercentage.tsv")

    val data: List[String] = prepareData(output)
    Util.writeToFile(data, s"$writeTo/$noisePercentage/data-noise-$noisePercentage.tsv")
  }

  def prepareData(input: Map[Long, HospTuple]): List[String] = {
    //    val data: Iterable[String] = input.map(d => s"${d._1.toString}\t${d._2.asString}")
    //    data.toList

    val predicates: Iterable[String] = input.map(t => {
      t._2.createPredicates(t._1)
    })
    predicates.toList
  }

  def prepareList(input: List[(Long, Int)]): List[String] = {

    val grouped: Map[Long, List[(Long, Int)]] = input.groupBy(_._1)
    val normalized: Map[Long, List[Int]] = grouped.map(e => {
      val attrs: List[Int] = e._2.map(_._2)
      (e._1, attrs)
    })

    val output: Iterable[String] = normalized.map(tuple => {
      s"""${tuple._1.toString}\t${tuple._2.mkString("\t")}"""
    })
    output.toList
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



  //setters

  def makeNoisyhospitalName(str: String) {
    this.hospitalName = hospitalName + str
  }

  def makeNoisyaddress(str: String) {
    this.address = this.address + str
  }

  def makeNoisycity(str: String) {
    this.city = this.city + str
  }

  def makeNoisystate(str: String) {
    this.state = this.state + str
  }

  def makeNoisyzipCode(str: String) {
    this.zipCode = this.zipCode + str
  }

  def makeNoisycountyName(str: String) {
    this.countyName = this.countyName + str
  }

  def makeNoisyphoneNumber(str: String) {
    this.phoneNumber = this.phoneNumber + str
  }

  def makeNoisycondition(str: String) {
    this.condition = this.condition + str
  }

  def makeNoisymeasureID(str: String) {
    this.measureID = this.measureID + str
  }

  def makeNoisymeasureName(str: String) {
    this.measureName = this.measureName + str
  }

  def makeNoisyscore(str: String) {
    this.score = this.score + str
  }

  def makeNoisysample(str: String) {
    this.sample = this.sample + str
  }

  def makeNoisyfootnote(str: String) {
    this.footnote = this.footnote + str
  }

  def makeNoisymeasureStartDate(str: String) {
    this.measureStartDate = this.measureStartDate + str
  }

  def makeNoisymeasureEndDate(str: String) {
    this.measureEndDate = this.measureEndDate + str
  }


  def asString: String = {

    s"""${this.providerID}\t$hospitalName\t$address\t$city\t$state\t$zipCode\t$countyName\t$phoneNumber\t$condition\t$measureID\t$measureName\t$score\t$sample\t$footnote\t$measureStartDate\t$measureEndDate"""
  }

  def insertNoise(attrs: List[Int]): this.type = {
    val noise = "typo"
    attrs foreach (i => i match {

      case 2 => makeNoisyhospitalName(noise)
      case 3 => makeNoisyaddress(noise)
      case 4 => makeNoisycity(noise)
      case 5 => makeNoisystate(noise)
      case 6 => makeNoisyzipCode(noise)
      case 7 => makeNoisycountyName(noise)
      case 8 => makeNoisyphoneNumber(noise)
      case 9 => makeNoisycondition(noise)
      case 10 => makeNoisymeasureID(noise)
      case 11 => makeNoisymeasureName(noise)
      case 12 => makeNoisyscore(noise)
      case 13 => makeNoisysample(noise)
      case 14 => makeNoisyfootnote(noise)
      case 15 => makeNoisymeasureStartDate(noise)
      case 16 => makeNoisymeasureEndDate(noise)
    })
    this
  }

  val createPredicates: (Long) => String = (idx) => {
    import Util._
    s"""
       |providerID("$idx", "${normalizeGroundAtom(this.providerID)}")
       |hospitalName("$idx", "${normalizeGroundAtom(this.hospitalName)}")
       |address("$idx", "${normalizeGroundAtom(this.address)}")
       |city("$idx", "${normalizeGroundAtom(this.city)}")
       |state("$idx", "${normalizeGroundAtom(this.state)}")
       |zipCode("$idx", "${normalizeGroundAtom(this.zipCode)}")
       |countyName("$idx", "${normalizeGroundAtom(this.countyName)}")
       |phoneNumber("$idx", "${normalizeGroundAtom(this.phoneNumber)}")
       |condition("$idx", "${normalizeGroundAtom(this.condition)}")
       |measureID("$idx", "${normalizeGroundAtom(this.measureID)}")
       |measureName("$idx", "${normalizeGroundAtom(this.measureName)}")
       |score("$idx", "${normalizeGroundAtom(this.score)}")
       |sample("$idx", "${normalizeGroundAtom(this.sample)}")
       |footnote("$idx", "${normalizeGroundAtom(this.footnote)}")
       |measureStartDate("$idx", "${normalizeGroundAtom(this.measureStartDate)}")
       |measureEndDate("$idx", "${normalizeGroundAtom(this.measureEndDate)}")
     """.stripMargin
  }
}
