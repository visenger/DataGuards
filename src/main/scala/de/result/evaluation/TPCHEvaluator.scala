package de.result.evaluation

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, Path}

import com.typesafe.config.{ConfigFactory, Config}
import de.data.preparation.{NoiseHOSP2, TPCHTuple}
import de.util.{Util, StringUtil}
import de.util.Util._

import scala.collection.immutable.{IndexedSeq, Iterable}
import scala.collection.mutable
import scala.io.Source

/**
 * Created by visenger on 18/05/15.
 */

/*
//CFD
eqNames \t 2
eqAddr \t 3
eqNatkey \t 4
eqPhone \t 5
eqAcc \t 6
eqMrkt \t 7

//MD
matchPhone \t 5
matchAddr \t 3

//interleaved
shouldMatchPhone \t 5
shouldMatchAddr \t 3

example:
eqPhone("358", "29-797-538-3006typo", "426", "29-797-538-3006")
eqAddr("2", "u5lVPzNeS1z2TcfehzgZFHXtHyxNJHU", "18", "u5lVPzNeS1z2TcfehzgZFHXtHyxNJHUtypo")
eqAcc("137", "5583.93typo", "81", "5583.93")
eqAddr("144", "b06rg6Cl5W", "152", "b06rg6Cl5Wtypo")
eqNatkey("834", "11", "878", "11typo")
matchPhone("860", "868")
shouldMatchAddr("836", "888")
shouldMatchAddr("836", "820")
*/
class TPCHEvaluator() {
  val config: Config = ConfigFactory.load()
  val resultFolder: String = config.getString("data.tpch.resultFolder")
  val evaluationFolder: String = config.getString("data.tpch.evalFolder")

  val dataSetSizes = Array(500, 1000, 10000, 20000, 30000, 40000, 50000, 70000, 90000, 100000)

  def generatePlots(): Unit = {
    import Util._
    for (j <- dataSetSizes) {

      val path: Path = Paths.get(s"$evaluationFolder/evaluation-$j-datasize.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"NOISE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      for (i <- 2 to 10;
           if i % 2 == 0) yield {
        val (f1_cfd, f1_md, f1_cfdMd, runtime) = computeF1(i, j)
        val line = s"$i\t${round(f1_cfd)(4)}\t${round(f1_md)(4)}\t${round(f1_cfdMd)(4)}\t${round(runtime)(4)}"

        writer.write(s"$line\n")
      }
      writer.close()

    }

  }

  def generatePlots2(): Unit = {
    import Util._
    for (i <- 2 to 10;
         if i % 2 == 0) {

      val path: Path = Paths.get(s"$evaluationFolder/evaluation-$i-noise.tsv")
      val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

      val header = s"DATASIZE\tCFDF1\tMDF1\tCFDMDF1\tTIME"
      writer.write(s"$header\n")

      for (j <- dataSetSizes) yield {
        val (f1_cfd, f1_md, f1_cfdMd, runtime) = computeF1(i, j)
        val line = s"$j\t${round(f1_cfd)(4)}\t${round(f1_md)(4)}\t${round(f1_cfdMd)(4)}\t${round(runtime)(4)}"

        writer.write(s"$line\n")
      }
      writer.close()
    }
  }


  def runEvaluation: Unit = {

    for {i <- 2 to 10
         j <- dataSetSizes
         if i % 2 == 0} {

      val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-dataSize-$j-noise-$i.tsv").getLines().toList

      val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)

      val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLinesDictionary(noiseDictionary, TPCHTuple.getAllAttributeIdxs())


      val lines: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results/output-tpch-dataSize-$j-noise-$i.db").getLines().toList
      val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

      /* Start evaluation computation */

      //cfd:
      val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getCFDResults(groupedByAttr)
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
      val f1_cfd = (2 * precision_cfd * recall_cfd) / (precision_cfd + recall_cfd)
      // println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")

      //md:
      val md: Map[Int, List[IDTuple]] = getMDResults(groupedByAttr)
      var tps_md = 0
      var fps_md = 0
      var fns_md = 0
      for (m <- md) {
        val attrId = m._1
        val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())
        val foundElents: List[IDTuple] = m._2

        val (tp, fp, fn) = computeFMeasure(foundElents, goldStandard)
        tps_md += tp
        fps_md += fp
        fns_md += fn
      }

      val precision_md = calculate(tps_md, fps_md)
      val recall_md = calculate(tps_md, fns_md)

      val f1_md = (2 * precision_md * recall_md) / (precision_md + recall_md)

      // println(s" data size = $j; noise = $i%; task= md only;  precision= $precision_md; recall= $recall_md; F1 = $f1_md")

      //cfd and md interleaved:
      val cfdAndMd: Map[Int, List[IDTuple]] = getCFD_MDResults(groupedByAttr)
      var tps_cfdMd = 0
      var fps_cfdMd = 0
      var fns_cfdMd = 0
      for (cm <- cfdAndMd) {
        val attrId = cm._1
        val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())
        val foundElents: List[IDTuple] = cm._2

        val (tp, fp, fn) = computeFMeasure(foundElents, goldStandard)
        tps_cfdMd += tp
        fps_cfdMd += fp
        fns_cfdMd += fn
      }

      val precision_cfdMd = calculate(tps_cfdMd, fps_cfdMd)
      val recall_cfdMd = calculate(tps_cfdMd, fns_cfdMd)

      val f1_cfdMd = (2 * precision_cfdMd * recall_cfdMd) / (precision_cfdMd + recall_cfdMd)

      // time execution:
      val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results/results-tpch-dataSize-$j-noise-$i.txt").getLines().toList
      val runtime: Double = getTimeInSeconds(resultsLog)

      // println(s" data size = $j; noise = $i%; task= cfd & md interleaved;  precision= $precision_cfdMd; recall= $recall_cfdMd; F1 = $f1_cfdMd; time=$runtime")

      println(s"$i&$j&${round(runtime)(4)}&${round(precision_cfd)(4)}&${round(recall_cfd)(4)}&${round(f1_cfd)(4)}&${round(precision_md)(4)}&${round(recall_md)(4)}&${round(f1_md)(4)}&${round(precision_cfdMd)(4)}&${round(recall_cfdMd)(4)}&${round(f1_cfdMd)(4)}")
    }


  }


  private def computeF1(i: Int, j: Int): (Double, Double, Double, Double) = {


    val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-dataSize-$j-noise-$i.tsv").getLines().toList

    val noiseDictionary: Map[Int, List[Int]] = getNoiseDict(logs)

    val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLinesDictionary(noiseDictionary, TPCHTuple.getAllAttributeIdxs())


    val lines: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results/output-tpch-dataSize-$j-noise-$i.db").getLines().toList
    val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

    /* Start evaluation computation */

    //cfd:
    val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = getCFDResults(groupedByAttr)
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
    val f1_cfd = (2 * precision_cfd * recall_cfd) / (precision_cfd + recall_cfd)
    // println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")

    //md:
    val md: Map[Int, List[IDTuple]] = getMDResults(groupedByAttr)
    var tps_md = 0
    var fps_md = 0
    var fns_md = 0
    for (m <- md) {
      val attrId = m._1
      val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())
      val foundElents: List[IDTuple] = m._2

      val (tp, fp, fn) = computeFMeasure(foundElents, goldStandard)
      tps_md += tp
      fps_md += fp
      fns_md += fn
    }

    val precision_md = calculate(tps_md, fps_md)
    val recall_md = calculate(tps_md, fns_md)

    val f1_md = (2 * precision_md * recall_md) / (precision_md + recall_md)

    // println(s" data size = $j; noise = $i%; task= md only;  precision= $precision_md; recall= $recall_md; F1 = $f1_md")

    //cfd and md interleaved:
    val cfdAndMd: Map[Int, List[IDTuple]] = getCFD_MDResults(groupedByAttr)
    var tps_cfdMd = 0
    var fps_cfdMd = 0
    var fns_cfdMd = 0
    for (cm <- cfdAndMd) {
      val attrId = cm._1
      val goldStandard: List[Int] = attrToLineDictionary.getOrElse(attrId, List())
      val foundElents: List[IDTuple] = cm._2

      val (tp, fp, fn) = computeFMeasure(foundElents, goldStandard)
      tps_cfdMd += tp
      fps_cfdMd += fp
      fns_cfdMd += fn
    }

    val precision_cfdMd = calculate(tps_cfdMd, fps_cfdMd)
    val recall_cfdMd = calculate(tps_cfdMd, fns_cfdMd)

    val f1_cfdMd = (2 * precision_cfdMd * recall_cfdMd) / (precision_cfdMd + recall_cfdMd)

    // time execution:
    val resultsLog: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/results/results-tpch-dataSize-$j-noise-$i.txt").getLines().toList
    val runtime: Double = getTimeInSeconds(resultsLog)

    (f1_cfd, f1_md, f1_cfdMd, runtime)
  }


  private def getTimeInSeconds(resultsLog: List[String]): Double = {
    val runtimeStr: String = resultsLog.filter(_.startsWith("It")).head

    val pattern = ".+\\s+took\\s+(\\d+)\\s+milliseconds".r
    val pattern(t) = runtimeStr

    val time: Double = t.trim.toDouble
    val seconds: Double = time / 1000.0
    seconds
  }

  private def getCFD_MDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[IDTuple]] = {
    for (g <- groupedByAttr; if g._1.startsWith("should")) yield {
      val atoms = deduplicateArray(g._2)
      val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
      (attrId, atoms)
    }
  }

  private def getMDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[IDTuple]] = {
    for (g <- groupedByAttr; if g._1.startsWith("match")) yield {
      val atoms = deduplicateArray(g._2)
      val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
      (attrId, atoms)
    }
  }

  private def getCFDResults(groupedByAttr: Map[String, List[String]]): Map[Int, List[(AttrAtom, AttrAtom)]] = {
    for (g <- groupedByAttr; if g._1.startsWith("eq")) yield {
      val atoms = deduplicateTuples(g._2)
      val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
      (attrId, atoms)
    }
  }

  private def getNoiseDict(logs: List[String]): Map[Int, List[Int]] = {
    logs.map(l => {
      val parts: Array[String] = l.split("\\t")
      val lineId: Int = parts.head.trim.toInt
      val attrIds: List[Int] = StringUtil.convertToInt(parts.tail.toList)
      (lineId, attrIds)
    }).toMap
  }

  def calculate(first: Int, second: Int) = first.toDouble / (first.toDouble + second.toDouble)

  private def computeFMeasure(input: List[IDTuple], goldStandard: List[Int]): (Int, Int, Int) = {
    // (AttrAtom(364,31-579-682-9907typo),AttrAtom(396,31-579-682-9907))
    // AttrAtom(id, value)

    val tp = mutable.Set[Int]()
    val fp = mutable.Set[Int]()

    for (tuple <- input) {

      val firstId = tuple.id1.toInt
      val secondId = tuple.id2.toInt

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

    //    val precision = tp.size.toDouble / (tp.size + fp.size).toDouble
    //    val recall = tp.size.toDouble / (tp.size + fn.size).toDouble

    // (precision, recall)
    (tp.size, fp.size, fn.size)

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


  private def generateAttrToLinesDictionary(noiseDictionary: Map[Int, List[Int]], idxs: List[Int]): Map[Int, List[Int]] = {

    val attrToLineTuples: List[(Int, List[Int])] = for {attr <- idxs} yield {

      val linesForAttr: Iterable[Int] = for {noise <- noiseDictionary;
                                             if noise._2.contains(attr)} yield noise._1

      (attr, linesForAttr.toList)

    }
    attrToLineTuples.toMap
  }


  private def deduplicateTuples(attrLines: List[String]): List[(AttrAtom, AttrAtom)] = {

    val mapi: List[Tuple2[AttrAtom, AttrAtom]] = attrLines.map(a => {
      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
      val Array(id1, val1, id2, val2) = innerPart.split(',')
      Tuple2(AttrAtom(id1.trim, val1.trim), AttrAtom(id2.trim, val2.trim))
    })

    //magic with deduplication ;) transform the list of tuples into a set. See MyTuple implementation for the equal method
    val duplicatesBuffer = mapi.map(MyTuple.apply).toSet.toList.map((mt: MyTuple[AttrAtom]) => mt.t)
    val diff: List[(AttrAtom, AttrAtom)] = mapi.diff(duplicatesBuffer)

    diff
  }


  private def deduplicateArray(attrLines: List[String]): List[IDTuple] = {

    val attrList: List[IDTuple] = attrLines.map(a => {
      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
      val Array(id1, id2) = innerPart.split(',')
      IDTuple(id1.trim, id2.trim)
    })
    val dedupicatedAttrs: Set[IDTuple] = attrList.toSet
    dedupicatedAttrs.toList
  }

}

object PlaygroundEvaluator extends App {
  //  new TPCHEvaluator().runEvaluation
  new TPCHEvaluator().generatePlots()
//  new TPCHEvaluator().generatePlots2()
}
