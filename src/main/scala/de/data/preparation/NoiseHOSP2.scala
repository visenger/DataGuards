package de.data.preparation

import java.io.File

import com.google.common.collect.Maps
import com.typesafe.config.ConfigFactory
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.{ListMap, Iterable}
import scala.io.{BufferedSource, Source}
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
  val chunkSize = 1000


  def inject() = {

    val dir: File = new File(datapath)


    for {file <- dir.listFiles() if file.getName.startsWith("hosp")} {
      val input: Map[Long, Hosp2Tuple] = readData(file.getAbsolutePath)
      val noiseElements: List[(Long, Int)] = calculateNoiseElements(input.size)
      val output: Map[Long, Hosp2Tuple] = insertNoise(input, noiseElements)


      val fileName = file.getName.takeWhile(_ != '.')

      /* * lookbehind solution * */

      val digitRegex = """(?<=-)\d+(?=-)""".r
      val digitOpt: Option[String] = digitRegex.findFirstIn(fileName)

      val dataSize: String = digitOpt match {
        case Some(x) => digitOpt.get
        case None => ""
      }
      val logNoise: List[String] = prepareList(noiseElements)
      Util.writeToFile(logNoise, s"$writeTo/$noisePercentage/$dataSize/log-$fileName-noise-$noisePercentage.tsv")

      //      val data: List[String] = prepareData(output)
      //      Util.writeToFile(data, s"$writeTo/$noisePercentage/data-noise-$fileName-$noisePercentage.db")

      val dataChunked: List[(Int, List[String])] = prepareDataChunked(output)

      val predZipTSV = new ZipData().predicatesZipTSV

      dataChunked.foreach(chunk => {
        val chunkNo = chunk._1
        val data = chunk._2 ::: predZipTSV
        Util.writeToFile(data, s"$writeTo/$noisePercentage/$dataSize/data-$fileName-noise-$noisePercentage-chunk-$chunkNo.db")
      })


      val runners: String = generateScript(dataSize, dataChunked)
      Util.writeStrToFile(runners, s"$writeTo/$noisePercentage/$dataSize/run-rockit-on-hosp-noise-$noisePercentage-dataSize-$dataSize.sh")


    }
  }


  private def generateScript(dataSize: String, dataChunked: List[(Int, List[String])]): String = {

    val runRockitStrs: List[String] = dataChunked.map(chunk => {
      val chunkNo = chunk._1
      s"java -jar /opt/rockit/rockit-0.5.277.jar -input /home/larysa/rockit/EXPERIMENTS-2/HOSP-2/hosp.mln -data /home/larysa/rockit/EXPERIMENTS-2/HOSP-2/$noisePercentage/$dataSize/data-hosp-$dataSize-k-noise-$noisePercentage-chunk-$chunkNo.db -output /home/larysa/rockit/EXPERIMENTS-2/HOSP-2/$noisePercentage/$dataSize/results/output-data-hosp-$dataSize-k-noise-$noisePercentage-chunk-$chunkNo.db -para /home/larysa/rockit/EXPERIMENTS-2/HOSP-2/rockit.properties"
    })
    val runners: String = runRockitStrs.mkString("\n")

    s"""
       |#!/bin/bash
       |
       |$runners
        |
        |cat results/output-data-*.db > results/output-data-hosp-$dataSize-k-noise-$noisePercentage.db
                                                                                                     |
                                                                                                     |echo $$?
     """.stripMargin
  }

  def createScript(runners: String): String = {
    s"""
       |#!/bin/bash
       |
       |	   $runners
        |
        |echo $$?
     """.stripMargin
  }

  def readData(path: String): Map[Long, Hosp2Tuple] = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("HOSP")
    val sc: SparkContext = new SparkContext(sparkConf)

    val header: String = sc.textFile(path).first()

    // val filteredHeader: RDD[String] = sc.textFile(path)
    val filteredHeader: RDD[String] = sc.textFile(path).filter(!_.equals(header))


    val tupled: RDD[Hosp2Tuple] = filteredHeader.map(line => {

      val Array(providerID, _, _, _, _, city, state, zipCode, _, phoneNumber, _, _, _, condition, measureID, measureName, _, _, stateAvg) = line.split(',')

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
    // todo: separate file zip.db
    val predZipTSV = new ZipData().predicatesZipTSV
    predicates.toList ::: predZipTSV
  }

  def prepareDataChunked(input: Map[Long, Hosp2Tuple]): List[(Int, List[String])] = {

    val sortedInput: Map[Long, Hosp2Tuple] = ListMap(input.toSeq.sortBy(_._2.providerID): _*)

    val chunked: List[Map[Long, Hosp2Tuple]] = sortedInput.grouped(chunkSize).toList

    val indexedChunks: List[(Int, Map[Long, Hosp2Tuple])] = chunked.zipWithIndex.map(_.swap)

    val predicateChunks: List[(Int, List[String])] = indexedChunks.map(chunk => {

      val predicates: Iterable[String] = chunk._2.map(t => {
        t._2.createPredicates(t._1)
      })
      (chunk._1, predicates.toList)

    })
    predicateChunks
  }

  /**
   * inserted noise as log list preparation
   * @param input List of inserted errors line_id: attribute_id */
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
