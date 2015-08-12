package de.result.evaluation

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, Path}
import javax.swing.InternalFrameFocusTraversalPolicy

import com.typesafe.config.{ConfigFactory, Config}
import de.data.preparation.NoiseHOSP2
import de.util.Util._
import de.util.{Util, StringUtil}

import scala.collection.immutable.{IndexedSeq, Iterable}
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

/**
 * Created by visenger on 23/05/15.
 *
 * cdf:
 * eqHospitalNameH(hid, name, hid, name)
eqAddressH(hid, address, hid, address)
eqCityH(hid, city, hid, city)
eqStateH(hid, state, hid, state)
eqZipCodeH(hid, code, hid, code)
eqCountryNameH(hid, country, hid, country)
eqPhoneNumberH(hid, number, hid, number)
eqMeasureNameH(hid, measurename, hid, measurename)
eqConditionH(hid, condition, hid, condition)

 eqStateH(hid, state, hid, state)

 md:
shouldBeStateH(hid, state, state)

 cfd and md interleaved:
newStateH(hid, state)

 ------

 eqPhoneNumberH("583", "2562651000", "593", "2562651000typo")
eqConditionH("107", "Pneumonia", "360", "Pneumoniatypo")
eqConditionH("223", "Surgical Infection Preventiontypo", "695", "Surgical Infection Prevention")
eqCityH("677", "CLANTON", "671", "CLANTONtypo")
eqConditionH("575", "Heart Failure", "675", "Heart Failuretypo")
eqMeasureNameH("487", "Heart Attack Patients Given Aspirin at Dischargetypo", "18", "Heart Attack Patients Given Aspirin at Discharge")
eqConditionH("564", "Pneumoniatypo", "876", "Pneumonia")
eqPhoneNumberH("367", "2059344011", "375", "2059344011typo")
eqMeasureNameH("12", "surgery patients whose doctors ordered treatments to prevent blood clots after certain types of surgeriestypo", "184", "surgery patients whose doctors ordered treatments to prevent blood clots after certain types of surgeries")
eqPhoneNumberH("510", "3346366221", "536", "3346366221typo")

 */
class HOSP2Evaluator() {

  val config: Config = ConfigFactory.load()
  val resultFolder: String = config.getString("data.hosp2.resultFolder")
  val evaluaitonFolder = config.getString("data.hosp2.evalFolder")
  val dataSetSizes = Array(1, 10, 20, 30, 40, 80, 90, 100)
  // val dataSetSizes = Array(80 /*, 40, 80, 90, 100*/)

  def extractAndWriteRuntimesHOSP(): Unit = {
    for {i <- 2 to 10
         if i % 2 == 0} {
      val path: Path = Paths.get(s"$evaluaitonFolder/runtime-$i-noise.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"DATASIZE\tTIME"
      writer.write(s"$header\n")
      for (j <- dataSetSizes) {
        val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
        val runtime: Double = getTimeInSeconds(resultsLog)
        writer.write(s"$j\t$runtime\n")
      }
      writer.close()
    }
  }


  def runEvaluator(): Unit = {
    import Util._
    val evalResults: IndexedSeq[String] = for {i <- 2 to 10
                                               j <- dataSetSizes
                                               if i % 2 == 0} yield {

      //noise logs
      val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-hosp-$j-k-noise-$i.tsv").getLines().toList
      val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)
      val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLineDictionary(noiseDictionary, NoiseHOSP2.getAllAttributeIdxs())
      //attrToLineDictionary.foreach(n => println( s"""${n._1}  ${n._2.mkString(" ")}"""))

      val lines = Source.fromFile(s"$resultFolder/$i/$j/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList
      val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

      //cfd:
      val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getCFDResults(groupedByAttr)
      val (precision_cfd, recall_cfd, f1_cfd) = evaluateCFDs(cfd, attrToLineDictionary)
      //println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")

      //md:
      val md: Map[Int, List[AttrAtom3]] = getMDResults(groupedByAttr)
      val (precision_md, recall_md, fMeasure_md) = evaluateMDs(md, attrToLineDictionary)
      //println(s" data size = $j; noise = $i%; task= md;  precision= $precision_md; recall= $recall_md; F1 = $fMeasure_md")

      //md and cfd interleaved:
      val cfdMd: Map[Int, List[TupleIdValue]] = getCFD_MDResults(groupedByAttr)
      val (precision_cfdMd, recall_cfdMd, fMeasure_cfdMd) = evaluateCFD_MDResults(cfdMd, attrToLineDictionary)
      //println(s" data size = $j; noise = $i%; task= cfd and md interleaved;  precision= $precision_cfdMd; recall= $recall_cfdMd; F1 = $fMeasure_cfdMd")

      // time execution:
      val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
      val runtime: Double = getTimeInSeconds(resultsLog)

      println(s"$i&$j&${round(runtime)(4)}&${round(precision_cfd)(4)}&${round(recall_cfd)(4)}&${round(f1_cfd)(4)}&${round(precision_md)(4)}&${round(recall_md)(4)}&${round(fMeasure_md)(4)}&${round(precision_cfdMd)(4)}&${round(recall_cfdMd)(4)}&${round(fMeasure_cfdMd)(4)}")

      val evalStr = s"$i\t$j\t${round(precision_cfd)(4)}\t${round(recall_cfd)(4)}\t${round(f1_cfd)(4)}\t${round(precision_md)(4)}\t${round(recall_md)(4)}\t${round(fMeasure_md)(4)}\t${round(precision_cfdMd)(4)}\t${round(recall_cfdMd)(4)}\t${round(fMeasure_cfdMd)(4)}\t${round(runtime)(4)}"
      evalStr
    }
    val header = s"%noi\tDATA SIZE\tCFD P\tCFD R\tCFD F1\tMD P\tMD R\tMD F1\tCFD+MD P\tCFD+MD R\tCFD+MD F1\tTIME"

    //Util.writeToFileWithHeader(header, evalResults.toList, s"$resultFolder/evaluation-hosp2.tsv")

  }

  def runEvaluatorExecutionOrderExperiments(): Unit = {
    import Util._
    val resultFolderMdCfd = config.getString("data.hosp2.resultFolderMdCfd")
    val resultFolderCfdMd = config.getString("data.hosp2.resultFolderCfdMd")
    val resultFolderJointly = config.getString("data.hosp2.resultFolderJointly")

    val evalResults: IndexedSeq[String] = for {i <- 2 to 10
                                               j <- dataSetSizes
                                               if i % 2 == 0} yield {

      //noise logs -> used by all
      val logs: List[String] = Source.fromFile(s"$resultFolderMdCfd/$i/$j/log-hosp-$j-k-noise-$i.tsv").getLines().toList
      val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)
      val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLineDictionary(noiseDictionary, NoiseHOSP2.getAllAttributeIdxs())

      // CFD -> MD
      val linesCfdMd: List[String] = Source.fromFile(s"$resultFolderCfdMd/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList
      val groupedByAttrCfdMd: Map[String, List[String]] = groupByPredicateName(linesCfdMd)

      val cfdmd: Map[Int, List[AttrAtom3]] = getCFDMDResults(groupedByAttrCfdMd)
      val (p_cfdmd, r_cfdmd, f1_cfdmd) = evaluateMDs(cfdmd, attrToLineDictionary)

      // CFD->MD time execution:
      val resultsLogCfdMd: List[String] = Source.fromFile(s"$resultFolderCfdMd/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
      val runtimeCfdMd: Double = getTimeInSeconds(resultsLogCfdMd)

      /*++++++++++*/
      // MD ->CFD
      val linesMdCfd: List[String] = Source.fromFile(s"$resultFolderMdCfd/result/output-data-hosp-$j-k-noise-$i.db").getLines().toList
      val groupedByAttrMdCfd: Map[String, List[String]] = groupByPredicateName(linesMdCfd)

      val mdcfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getMDCFDResults(groupedByAttrMdCfd)
      val (p_mdcfd, r_mdcfd, f1_mdcfd) = evaluateCFDs(mdcfd, attrToLineDictionary)

      // CFD->MD time execution:
      val resultsLogMdCfd: List[String] = Source.fromFile(s"$resultFolderMdCfd/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
      val runtimeMdCfd: Double = getTimeInSeconds(resultsLogMdCfd)

      /*++++++++++*/
      // JOINTLY
      val linesJointly: List[String] = Source.fromFile(s"$resultFolderJointly/$i/$j/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList
      val groupedByAttrJointly: Map[String, List[String]] = groupByPredicateName(linesJointly)

      val jointly: Map[Int, List[TupleIdValue]] = getJointlyResults(groupedByAttrJointly)
      val (p_jointly, r_jointly, f1_jointly) = evaluateCFD_MDResults(jointly, attrToLineDictionary)

      // JOINTLY time execution:
      val resultsLogJointly: List[String] = Source.fromFile(s"$resultFolderJointly/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
      val runtimeJointly: Double = getTimeInSeconds(resultsLogJointly)

//      val evalStr = s"$i\t$j\t${round(precision_cfd)(4)}\t${round(recall_cfd)(4)}\t${round(f1_cfd)(4)}\t${round(precision_md)(4)}\t${round(recall_md)(4)}\t${round(fMeasure_md)(4)}\t${round(precision_cfdMd)(4)}\t${round(recall_cfdMd)(4)}\t${round(fMeasure_cfdMd)(4)}\t${round(runtime)(4)}"
//      evalStr
      "placeholder"
    }
    val header = s"%noi\tDATA SIZE\tCFD P\tCFD R\tCFD F1\tMD P\tMD R\tMD F1\tCFD+MD P\tCFD+MD R\tCFD+MD F1\tTIME"

    //Util.writeToFileWithHeader(header, evalResults.toList, s"$resultFolder/evaluation-hosp2.tsv")

  }


  private def groupByPredicateName(linesCfdMd: List[String]): Map[String, List[String]] = {
    linesCfdMd.groupBy(e => e.takeWhile(_ != '('))
  }

  def generatePlots(): Unit = {
    import Util._
    for (j <- dataSetSizes) {

      val path: Path = Paths.get(s"$evaluaitonFolder/evaluation-$j-datasize.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"NOISE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      for (i <- 2 to 10;
           if i % 2 == 0) yield {

        //noise logs
        val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-hosp-$j-k-noise-$i.tsv").getLines().toList
        val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)
        val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLineDictionary(noiseDictionary, NoiseHOSP2.getAllAttributeIdxs())
        //attrToLineDictionary.foreach(n => println( s"""${n._1}  ${n._2.mkString(" ")}"""))

        val lines = Source.fromFile(s"$resultFolder/$i/$j/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList
        val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

        //cfd:
        val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getCFDResults(groupedByAttr)
        val (precision_cfd, recall_cfd, f1_cfd) = evaluateCFDs(cfd, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")

        //md:
        val md: Map[Int, List[AttrAtom3]] = getMDResults(groupedByAttr)
        val (precision_md, recall_md, fMeasure_md) = evaluateMDs(md, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= md;  precision= $precision_md; recall= $recall_md; F1 = $fMeasure_md")

        //md and cfd interleaved:
        val cfdMd: Map[Int, List[TupleIdValue]] = getCFD_MDResults(groupedByAttr)
        val (precision_cfdMd, recall_cfdMd, fMeasure_cfdMd) = evaluateCFD_MDResults(cfdMd, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= cfd and md interleaved;  precision= $precision_cfdMd; recall= $recall_cfdMd; F1 = $fMeasure_cfdMd")

        // time execution:
        val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
        val runtime: Double = getTimeInSeconds(resultsLog)

        val line = s"$i\t${round(f1_cfd)(4)}\t${round(fMeasure_md)(4)}\t${round(fMeasure_cfdMd)(4)}\t${round(runtime)(4)}"

        writer.write(s"$line\n")
      }
      writer.close()

    }


    //Util.writeToFileWithHeader(header, evalResults.toList, s"$resultFolder/evaluation-hosp2.tsv")

  }

  def generatePlots2(): Unit = {
    import Util._
    for (i <- 2 to 10;
         if i % 2 == 0) {

      val path: Path = Paths.get(s"$evaluaitonFolder/evaluation-$i-noise.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"DATASIZE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      for (j <- dataSetSizes) yield {

        //noise logs
        val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-hosp-$j-k-noise-$i.tsv").getLines().toList
        val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)
        val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLineDictionary(noiseDictionary, NoiseHOSP2.getAllAttributeIdxs())
        //attrToLineDictionary.foreach(n => println( s"""${n._1}  ${n._2.mkString(" ")}"""))

        val lines = Source.fromFile(s"$resultFolder/$i/$j/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList
        val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

        //cfd:
        val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getCFDResults(groupedByAttr)
        val (precision_cfd, recall_cfd, f1_cfd) = evaluateCFDs(cfd, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")

        //md:
        val md: Map[Int, List[AttrAtom3]] = getMDResults(groupedByAttr)
        val (precision_md, recall_md, fMeasure_md) = evaluateMDs(md, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= md;  precision= $precision_md; recall= $recall_md; F1 = $fMeasure_md")

        //md and cfd interleaved:
        val cfdMd: Map[Int, List[TupleIdValue]] = getCFD_MDResults(groupedByAttr)
        val (precision_cfdMd, recall_cfdMd, fMeasure_cfdMd) = evaluateCFD_MDResults(cfdMd, attrToLineDictionary)
        //println(s" data size = $j; noise = $i%; task= cfd and md interleaved;  precision= $precision_cfdMd; recall= $recall_cfdMd; F1 = $fMeasure_cfdMd")

        // time execution:
        val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results-hosp-dataSize-$j-noise-$i.txt").getLines().toList
        val runtime: Double = getTimeInSeconds(resultsLog)

        val line = s"$j\t${round(f1_cfd)(4)}\t${round(fMeasure_md)(4)}\t${round(fMeasure_cfdMd)(4)}\t${round(runtime)(4)}"

        writer.write(s"$line\n")
      }
      writer.close()

    }


    //Util.writeToFileWithHeader(header, evalResults.toList, s"$resultFolder/evaluation-hosp2.tsv")

  }


  private def getTimeInSeconds(resultsLog: List[String]): Double = {
    val runtimeStr: String = resultsLog.filter(_.startsWith("running")).head

    val pattern = ".+\\s+took\\s+(\\d+)\\s+milliseconds".r
    val pattern(t) = runtimeStr

    val time: Double = t.trim.toDouble
    val seconds: Double = time / 1000.0
    seconds
  }

  private def evaluateCFDs(cfd: Map[Int, List[(AttrAtom, AttrAtom)]], attrToLineDictionary: Map[Int, List[Int]]): (Double, Double, Double) = {
    var tps_cfd = 0
    var fps_cfd = 0
    var fns_cfd = 0
    for (x <- cfd) {
      val attrId = x._1

      val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())

      val foundAtoms: List[(AttrAtom, AttrAtom)] = x._2

      val (tp, fp, fn) = computeFMeasureForAtoms(foundAtoms, goldStandard)

      tps_cfd += tp
      fps_cfd += fp
      fns_cfd += fn
    }
    val precision_cfd = calculate(tps_cfd, fps_cfd)
    val recall_cfd = calculate(tps_cfd, fns_cfd)

    val f1_cfd = computeFMeasure(precision_cfd, recall_cfd)
    (precision_cfd, recall_cfd, f1_cfd)
  }

  private def evaluateMDs(md: Map[Int, List[AttrAtom3]], attrToLineDictionary: Map[Int, List[Int]]): (Double, Double, Double) = {
    var tps_md = 0
    var fps_md = 0
    var fns_md = 0

    for (x <- md) {
      val attrId: Int = x._1
      val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())

      val foundAtoms: List[AttrAtom3] = x._2

      val (tp, fp, fn) = computeFMeasureForAtoms3(foundAtoms, goldStandard)
      tps_md += tp
      fps_md += fp
      fns_md += fn
    }

    val precision_md = calculate(tps_md, fps_md)
    val recall_md = calculate(tps_md, fns_md)
    val fMeasure_md: Double = computeFMeasure(precision_md, recall_md)
    (precision_md, recall_md, fMeasure_md)
  }

  private def evaluateCFD_MDResults(cfdMd: Map[Int, List[TupleIdValue]], attrToLineDictionary: Map[Int, List[Int]]): (Double, Double, Double) = {
    var tps_cfdMd = 0
    var fps_cfdMd = 0
    var fns_cfdMd = 0

    for (x <- cfdMd) {
      val attrId: Int = x._1
      val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())
      val foundAtoms: List[TupleIdValue] = x._2
      val (tp, fp, fn) = computeFMeasureForTupleIdValues(foundAtoms, goldStandard)
      tps_cfdMd += tp
      fps_cfdMd += fp
      fns_cfdMd += fn
    }

    val precision_cfdMd = calculate(tps_cfdMd, fps_cfdMd)
    val recall_cfdMd = calculate(tps_cfdMd, fns_cfdMd)
    val fMeasure_cfdMd = computeFMeasure(precision_cfdMd, recall_cfdMd)
    (precision_cfdMd, recall_cfdMd, fMeasure_cfdMd)
  }


  private def getCFD_MDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[TupleIdValue]] = {
    for (x <- groupedByAttr; if x._1.startsWith("new")) yield {
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      val atoms = generateTupleIdValues(x._2)
      (attrId, atoms)
    }
  }

  private def getJointlyResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[TupleIdValue]] = {
    for (x <- groupedByAttr; if x._1.startsWith("new")) yield {
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      val atoms = generateTupleIdValues(x._2)
      (attrId, atoms)
    }
  }

  private def getMDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[AttrAtom3]] = {
    for (x <- groupedByAttr; if x._1.startsWith("should")) yield {
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      val atoms = generateAttrAtoms3(x._2)
      (attrId, atoms)
    }
  }

  private def getCFDMDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[AttrAtom3]] = {
    for (x <- groupedByAttr; if x._1.startsWith("joint")) yield {
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      val atoms = generateAttrAtoms3(x._2)
      (attrId, atoms)
    }
  }

  private def getMDCFDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[(AttrAtom, AttrAtom)]] = {
    for (x <- groupedByAttr; if x._1.startsWith("eq")) yield {
      val tuples: List[(AttrAtom, AttrAtom)] = deduplicateTuples(x._2)
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      (attrId, tuples)
    }
  }

  private def getCFDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[(AttrAtom, AttrAtom)]] = {
    for (x <- groupedByAttr; if x._1.startsWith("eq")) yield {
      val tuples: List[(AttrAtom, AttrAtom)] = deduplicateTuples(x._2)
      val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
      (attrId, tuples)
    }
  }

  private def getNoiseDict(logs: List[String]): Map[Int, List[Int]] = {
    val noiseDictionary: Map[Int, List[Int]] = logs.map(l => {
      val strs: Array[String] = l.split("\\t")
      val lineNr: Int = strs.head.toInt
      val noisyAttrs: List[Int] = StringUtil.convertToInt(strs.tail.toList)
      (lineNr, noisyAttrs)
    }).toMap
    noiseDictionary
  }

  def computeFMeasureForTupleIdValues(foundAtoms: List[TupleIdValue], goldStandard: List[Int]): (Int, Int, Int) = {

    val tp = mutable.Set[Int]()
    val fp = mutable.Set[Int]()

    for (x <- foundAtoms) {
      val attrId: Int = x.id.trim.toInt
      val found: Boolean = goldStandard.contains(attrId)
      found match {
        case true => tp.add(attrId)
        case false => fp.add(attrId)
      }
    }
    val fn: Set[Int] = goldStandard.toSet.diff(tp)
    (tp.size, fp.size, fn.size)
  }

  private def computeFMeasure(precision: Double, recall: Double): Double = {
    (2 * precision * recall) / (precision + recall)
  }

  def computeFMeasureForAtoms3(foundAtoms: List[AttrAtom3], goldStandard: List[Int]): (Int, Int, Int) = {
    val tp = mutable.Set[Int]()
    val fp = mutable.Set[Int]()

    for (atom <- foundAtoms) {
      val id: Int = atom.id.trim.toInt
      val found: Boolean = goldStandard.contains(id)

      found match {
        case true => tp.add(id)
        case false => fp.add(id)
      }
    }

    val fn: Set[Int] = goldStandard.toSet.diff(tp)

    (tp.size, fp.size, fn.size)
  }

  def calculate(first: Int, second: Int) = first.toDouble / (first.toDouble + second.toDouble)

  private def generateAttrToLineDictionary(lineToAttrs: Map[Int, List[Int]], attrs: List[Int]): Map[Int, List[Int]] = {

    val attrToLines: List[(Int, List[Int])] = for (attr <- attrs) yield {
      val lineNrs: Iterable[Int] = for (l <- lineToAttrs; if l._2.contains(attr)) yield l._1
      (attr, lineNrs.toList)
    }
    attrToLines.toMap
  }

  private def generateTupleIdValues(strings: List[String]): List[TupleIdValue] = {
    import de.util.StringUtil._
    strings.map(a => {
      val innerPart: String = extractParameterString(a)
      val Array(id, newValue) = innerPart.split(',')
      TupleIdValue(id, newValue)
    })
  }


  private def generateAttrAtoms3(strings: List[String]): List[AttrAtom3] = {
    import de.util.StringUtil._
    strings.map(a => {
      val innerPart: String = extractParameterString(a)
      val Array(id, oldValue, newValue) = innerPart.split(',')
      AttrAtom3(id, oldValue, newValue)
    })
  }

  private def deduplicateTuples(attrLines: List[String]): List[(AttrAtom, AttrAtom)] = {
    import de.util.StringUtil._
    val mapi: List[Tuple2[AttrAtom, AttrAtom]] = attrLines.map(a => {
      val innerPart: String = extractParameterString(a)
      val Array(id1, val1, id2, val2) = innerPart.split(',')
      Tuple2(AttrAtom(id1.trim, val1.trim), AttrAtom(id2.trim, val2.trim))
    })

    //magic with deduplication ;) transform the list of tuples into a set. See MyTuple implementation for the equal method
    val duplicatesBuffer = mapi.map(MyTuple.apply).toSet.toList.map((mt: MyTuple[AttrAtom]) => mt.t)
    val diff: List[(AttrAtom, AttrAtom)] = mapi.diff(duplicatesBuffer)

    diff
  }

  private def computeFMeasureForAtoms(input: List[(AttrAtom, AttrAtom)], goldStandard: List[Int]): (Int, Int, Int) = {
    // (AttrAtom(364,31-579-682-9907typo),AttrAtom(396,31-579-682-9907))
    // AttrAtom(id, value)

    val tp = mutable.Set[Int]()
    val fp = mutable.Set[Int]()

    for ((first, second) <- input) {
      val firstId: Int = first.id.toInt
      val secondId: Int = second.id.toInt

      val firstInGoldStandard: Boolean = goldStandard.contains(firstId)
      val secondInGoldStandard: Boolean = goldStandard.contains(secondId)

      (firstInGoldStandard, secondInGoldStandard) match {
        case (true, true) => {
          fp.add(firstId)
          fp.add(secondId)
        } //fp
        case (true, false) => tp.add(firstId) // tp
        case (false, true) => tp.add(secondId) //tp
        case (false, false) => {
          fp.add(firstId)
          fp.add(secondId)
        } // fp
      }

    }
    val fn: Set[Int] = goldStandard.toSet.diff(tp)
    (tp.size, fp.size, fn.size)

  }


}

object PlaygroundHOSP2Eval extends App {
  new HOSP2Evaluator().extractAndWriteRuntimesHOSP()
  //new HOSP2Evaluator().runEvaluator()
  // new HOSP2Evaluator().generatePlots()
  // new HOSP2Evaluator().generatePlots2()
}


object PlotsDataSizeConfigurationGenerator extends App {

  //NOISE	CFDF1	MDF1	CFDMDF1	TIME

  val dataSetSizes = Array(1, 10, 20, 30, 40 /*, 80, 90, 100*/)

  val plotsForDataSize: Array[String] = for (i <- dataSetSizes) yield {
    val plotConfig = s"""{
      "plot": {
        "axis":"axis",
        "title": "HOSP Data Cleaning for ${i}k",
        "xlabel": "noise",
        "ylabel": "F1",
        "addplot": [
          {
            "table": "x=NOISE, y=CFDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-datasize.tsv"
          },
          {
            "table": "x=NOISE, y=MDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-datasize.tsv"
          },
          {
            "table": "x=NOISE, y=CFDMDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-datasize.tsv"
          }
        ],
        "legend": "$$cfd$$,$$md$$,$$cfd+md$$"
      }
    }"""
    plotConfig
  }
  val start = s"""{
  "plots": [
    """
  val sep =
    s""",
       |
     """.stripMargin
  val end = s"""]
               |}
     """.stripMargin
  val plotsConfig: String = plotsForDataSize.mkString(start, sep, end)


  print(plotsConfig)
}

object PlotsNoiseConfigGeneration extends App {

  //DATASIZE	CFDF1	MDF1	CFDMDF1	TIME

  val noise = Array(2, 4, 6, 8, 10)

  val plotsForNoise: Array[String] = for (i <- noise) yield {
    val plotConfig = s"""{
      "plot": {
        "axis":"axis",
        "title": "HOSP Data Cleaning for noise ${i}%",
        "xlabel": "data size in k",
        "ylabel": "F1",
        "addplot": [
          {
            "table": "x=DATASIZE, y=CFDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-noise.tsv"
          },
          {
            "table": "x=DATASIZE, y=MDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-noise.tsv"
          },
          {
            "table": "x=DATASIZE, y=CFDMDF1",
            "data": "/Users/visenger/data/HOSP2/evaluation/evaluation-$i-noise.tsv"
          }
        ],
        "legend": "$$cfd$$,$$md$$,$$cfd+md$$"
      }
    }"""
    plotConfig
  }
  val start = s"""{
  "plots": [
    """
  val sep =
    s""",
       |
     """.stripMargin
  val end = s"""]
               |}
     """.stripMargin
  val plotsConfig: String = plotsForNoise.mkString(start, sep, end)


  print(plotsConfig)

}

case class EvaluationLine(noi: String, dataSize: String, runtime: String, cfdP: String, cfdR: String, cfdF1: String, mdP: String, mdR: String, mdF1: String, interP: String, interR: String, interF1: String)

object DataForPlotsGenerator extends App {
  val config = ConfigFactory.load()
  val evaluaitonFolder = config.getString("data.hosp2.evalFolder")

  generatePlots()

  def generatePlots(): Unit = {
    val lines: List[String] = Source.fromFile(s"$evaluaitonFolder/tmp.txt").getLines().toList
    val evalResults: List[EvaluationLine] = lines.map(l => {
      val Array(noi, dataSize, runtime, cfdP, cfdR, cfdF1, mdP, mdR, mdF1, interlP, interlR, interlF1) = l.split("&")
      EvaluationLine(noi, dataSize, runtime, cfdP, cfdR, cfdF1, mdP, mdR, mdF1, interlP, interlR, interlF1)
    })
    val groupedByDataSize: Map[String, List[EvaluationLine]] = evalResults.groupBy(_.dataSize)

    groupedByDataSize.foreach(t => {
      val j = t._1.trim
      val path: Path = Paths.get(s"$evaluaitonFolder/evaluation-$j-datasize.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
      val header = s"NOISE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      val evals: List[EvaluationLine] = t._2

      evals.foreach(e => {
        val line = s"${e.noi}\t${e.cfdF1}\t${e.mdF1}\t${e.interF1}\t${e.runtime}"
        writer.write(s"$line\n")
      })

      writer.close()
    })


    val groupedByNoise: Map[String, List[EvaluationLine]] = evalResults.groupBy(_.noi)

    groupedByNoise.foreach(t => {
      val i = t._1.trim
      val path: Path = Paths.get(s"$evaluaitonFolder/evaluation-$i-noise.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"DATASIZE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      val evals: List[EvaluationLine] = t._2
      evals.foreach(e => {
        val line = s"${e.dataSize}\t${e.cfdF1}\t${e.mdF1}\t${e.interF1}\t${e.runtime}"
        writer.write(s"$line\n")
      })

      writer.close()
    })

  }
}