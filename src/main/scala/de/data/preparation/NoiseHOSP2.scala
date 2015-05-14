package de.data.preparation

import java.io.File

import com.typesafe.config.ConfigFactory
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.Iterable
import scala.util.Random

/**

 *
 * Injects noise into the hosp data set, which is used by NADEEF;
 */
class Hosp2NoiseInjector(val datapath: String, val noisePercentage: Int = 2, val writeTo: String) {

  implicit class Crossable[T](xs: Traversable[T]) {
    def cross[X](ys: Traversable[X]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val config = ConfigFactory.load()
  val configAttrCount: String = "data.hosp2.attrCount"

  def inject() = {

    val dir: File = new File(datapath)


    for {file <- dir.listFiles() if file.getName.startsWith("hospital")} {
      val input: Map[Long, Hosp2Tuple] = readData(file.getAbsolutePath)
      val noiseElements: List[(Long, Int)] = calculateNoiseElements(input.size)
      val output: Map[Long, Hosp2Tuple] = insertNoise(input, noiseElements)


      val fileName = file.getName.takeWhile(_ != '.')
      val logNoise: List[String] = prepareList(noiseElements)
      Util.writeToFile(logNoise, s"$writeTo/$noisePercentage/log-noise-$fileName-$noisePercentage.tsv")

      val data: List[String] = prepareData(output)
      Util.writeToFile(data, s"$writeTo/$noisePercentage/data-noise-$fileName-$noisePercentage.db")
    }
  }


  def readData(path: String): Map[Long, Hosp2Tuple] = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
    val sc: SparkContext = new SparkContext(sparkConf)

    val header: String = sc.textFile(path).first()

   // val filteredHeader: RDD[String] = sc.textFile(path)
   val filteredHeader: RDD[String] = sc.textFile(path).filter(!_.equals(header))


    val tupled: RDD[Hosp2Tuple] = filteredHeader.map(line => {

      val Array(providerID ,_ ,_ ,_ ,_ , city,state ,zipCode ,_,phoneNumber ,_ ,_ ,_,condition ,measureID ,measureName ,_,_ ,stateAvg ) = line.split(',')

      Hosp2Tuple(providerID.toString.replace('"', ' ').trim, city.toString, state.toString, zipCode.toString, phoneNumber.toString, condition.toString, measureID.toString, measureName.toString, stateAvg.toString)
    })


    val tuplesWithId: RDD[(Long, Hosp2Tuple)] = tupled.zipWithUniqueId().map(_.swap)
    val tuples: Map[Long, Hosp2Tuple] = tuplesWithId.collect().toMap
    sc.stop()

    tuples
  }

  def calculateNoiseElements(size: Int): List[(Long, Int)] = {

    val attrCount = config.getInt(configAttrCount)
    val totalElementsCount: Int = size * attrCount
    val noisyElementsCount: Int = totalElementsCount * noisePercentage / 100

    val attrsIdx = (2 to attrCount).toList
    val tuplesIdx = (0.toLong to size.toLong).toList

    val matrix: List[(Long, Int)] = tuplesIdx.cross(attrsIdx).toList

    val noisyElements: List[(Long, Int)] = Random.shuffle(matrix).take(noisyElementsCount)

    noisyElements
  }

  def insertNoise(input: Map[Long, Hosp2Tuple], noiseIdx: List[(Long, Int)]): Map[Long, Hosp2Tuple] = {

    val groupedByTupleIdx = noiseIdx.groupBy(_._1)

    val outputWithNoise: Map[Long, Hosp2Tuple] = input.map(i => {
      if (groupedByTupleIdx.contains(i._1)) {
        (i._1, insertNoiseInto(i._2, groupedByTupleIdx.getOrElse(i._1, List())))
      } else (i._1, i._2)
    })
    outputWithNoise
  }

  private def insertNoiseInto(tuple: Hosp2Tuple, idx: List[(Long, Int)]): Hosp2Tuple = {
    val attrs: List[Int] = idx.map(_._2)
    val noise: Hosp2Tuple = tuple.insertNoise(attrs)
    noise
  }

  def prepareData(input: Map[Long, Hosp2Tuple]): List[String] = {

    val predicates: Iterable[String] = input.map(t => {
      t._2.createPredicates(t._1)
    })
    val predZipTSV = new ZipData().predicatesZipTSV
    predicates.toList ::: predZipTSV
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


}

case class Hosp2Tuple(providerID: String,
                      var city: String,
                      var state: String,
                      var zipCode: String,
                      var phoneNumber: String,
                      var condition: String,
                      var measureID: String,
                      var measureName: String,
                      var stateAvg: String) {


  def makeNoisycity(str: String) {
    this.city = this.city + str
  }

  def makeNoisystate(str: String) {
    this.state = this.state + str
  }

  def makeNoisyzipCode(str: String) {
    this.zipCode = this.zipCode + str
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

  def makeNoisystateAvg(str: String) {
    this.stateAvg = this.stateAvg + str
  }


  def asString: String = {
    s"""${this.providerID}\t$city\t$state\t$zipCode\t$phoneNumber\t$condition\t$measureID\t$measureName\t$stateAvg"""
  }

  def insertNoise(attrs: List[Int]): this.type = {
    val noise = "typo"
    attrs foreach (i => i match {

      case 2 => makeNoisycity(noise)
      case 3 => makeNoisystate(noise)
      case 4 => makeNoisyzipCode(noise)
      case 5 => makeNoisyphoneNumber(noise)
      case 6 => makeNoisycondition(noise)
      case 7 => makeNoisymeasureID(noise)
      case 8 => makeNoisymeasureName(noise)
      case 9 => makeNoisystateAvg(noise)
    })
    this
  }


  val createPredicates: (Long) => String = (idx) => {
    import Util._
    s"""
       |providerNumberH("$idx", "${normalizeGroundAtom(this.providerID)}")
       |cityH("$idx", "${normalizeGroundAtom(this.city)}")
       |stateH("$idx", "${normalizeGroundAtom(this.state)}")
       |zipCodeH("$idx", "${normalizeGroundAtom(this.zipCode)}")
       |phoneNumberH("$idx", "${normalizeGroundAtom(this.phoneNumber)}")
       |conditionH("$idx", "${normalizeGroundAtom(this.condition)}")
       |measureCodeH("$idx", "${normalizeGroundAtom(this.measureID)}")
       |measureNameH("$idx", "${normalizeGroundAtom(this.measureName)}")
       |stateAvgH("$idx", "${normalizeGroundAtom(this.stateAvg)}")
     """.stripMargin
  }



}
