package de.result.evaluation


import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, Path}

import com.typesafe.config.ConfigFactory
import de.util.Util

import scala.collection.immutable.Iterable
import scala.io.Source
import scala.util.Random

/**
  * Created by visenger on 12/09/15.
  */
object MSAGEvaluator {

  def main(args: Array[String]) {
    var path: String = ConfigFactory.load.getString("data.msag.path")
    var folderId: String = "0"
    var id: String = "19525FF1"

    if (args.length == 3) {
      println("using arguments via cli")
      path = args(0)
      folderId = args(1)
      id = args(2)
    } else {
      path = ConfigFactory.load.getString("data.msag.path")
      folderId = "0"
      id = "19525FF1"
    }

    val regexList: List[String] = Source.fromFile(s"$path/$folderId/$id/regex-$id.txt").getLines().toList

    val toEvalList: List[String] = Source.fromFile(s"$path/$folderId/$id/output-$id.db").getLines().toList

    val allWeNeed: List[List[String]] = for (r <- regexList) yield {
      val selected: List[String] = toEvalList.filter(_.matches(r))
      selected
    }
    val selectedPredicates: List[String] = allWeNeed.flatten

    val markovLogicPredicates: List[MarkovLogicPredicate] = selectedPredicates.map(MarkovLogicPredicate.apply)

    val distinct: List[MarkovLogicPredicate] = markovLogicPredicates.distinct

    //println("markovLogicPredicates found total = " + markovLogicPredicates.size)


    val selected: Int = distinct.size
    //println("distinct / deduplicate found = " + selected)


    /*check against gold standard: */

    val reference: List[MarkovLogicPredicate] =
      Source.fromFile(s"$path/$folderId/$id/reference-$id.txt").getLines().map(MarkovLogicPredicate.apply).toList
    val correct: Int = reference.size

    val intersectPredicates: List[MarkovLogicPredicate] = reference.intersect(distinct)

    val tp: Int = intersectPredicates.size
    // println("intersectPredicates tp = " + tp)

    val fpElements: List[MarkovLogicPredicate] = distinct.diff(reference)

    val fp: Int = fpElements.size
    // println("fpElements fp = " + fp)

    val fnElements: List[MarkovLogicPredicate] = reference.diff(distinct)
    val fn: Int = fnElements.size
    // println("fnElements fn = " + fn)

    val precision = {
      selected match {
        case 0 => 0.0
        case _ => tp.toDouble / selected.toDouble
      }

    }
    val recall = {
      correct match {
        case 0 => 0.0
        case _ => tp.toDouble / correct.toDouble
      }

    }

    val F1 = {
      val sum: Double = precision + recall
      sum match {
        case 0.0 => 0.0
        case _ => (2 * precision * recall) / sum
      }

    }

    println(s"$folderId\t$id\t$precision\t$recall\t$F1")


  }

}

//i,AUTHOR,EDGES,REMOVED
case class EdgesStat(forlderNr: String, authorId: String, edgesTotal: Int, removed: Int)


class MarkovLogicPredicate(val name: String, val firstArg: String, val secondArg: String) {

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + name.hashCode + firstArg.hashCode + secondArg.hashCode
    result
  }

  override def equals(that: scala.Any): Boolean = {
    //todo: finish this: two predicates are equal if ...
    //predicate(a,b) should be equal to predicate(a, b) or predicate(b, a)
    that match {
      case that: MarkovLogicPredicate => {
        val predicate = that.asInstanceOf[MarkovLogicPredicate]
        val generally = that.canEqual(this) && this.hashCode == that.hashCode && predicate.name == this.name

        val specifically = {
          (predicate.firstArg == this.firstArg && predicate.secondArg == this.secondArg) ||
            (predicate.firstArg == this.secondArg && predicate.secondArg == this.firstArg)
        }
        generally && specifically
      }
      case _ => false
    }
  }

  override def toString: String = s"$name($firstArg, $secondArg)"

  def canEqual(that: Any): Boolean = that.isInstanceOf[MarkovLogicPredicate]
}

object MarkovLogicPredicate {
  def apply(raw: String): MarkovLogicPredicate = {
    val ExpectedPredicate = "(.+)\\(\"(.+)\", \"(.+)\"\\)".r
    val ExpectedPredicate(predicateName, firstArg, secondArg) = raw
    new MarkovLogicPredicate(predicateName, firstArg, secondArg)
  }
}

class F1Line(val folderNr: String, val authorId: String, val precision: Double, val recall: Double, val F1: Double) {
  override def toString: String = s"$folderNr\t$authorId\t$precision\t$recall\t$F1"
}

object F1Line {

  import de.util.Util._

  def apply(input: String): F1Line = {
    val Array(folderNr, authorId, precision, recall, f1) = input.split("\\t")
    new F1Line(folderNr, authorId, round(precision.toDouble)(4), round(recall.toDouble)(4), round(f1.toDouble)(4))
  }
}

object InterpolatedPrecisionCreator extends App {
  val path: String = "/Users/visenger/data/MSAG/results"
  val input: Iterator[String] = Source.fromFile(s"${path}/msag-probe.txt").getLines()
  val lines: List[F1Line] = input.map(F1Line.apply).toList
  val groupedByRecall: Map[Double, List[F1Line]] = lines.groupBy(_.recall)


  val interpolated: Iterable[F1Line] = groupedByRecall.map(t => {
    val maxPrecision: Double = t._2.map(_.precision).reduceLeft(_ max _)
    val maxList: List[F1Line] = t._2.filter(f => f.precision == maxPrecision)
    maxList.head
  })
  val sorted: List[F1Line] = interpolated.toList.sortWith((x, y) => x.F1 < y.F1)

  val interpolatedPrecision: List[String] = sorted.toList.map(_.toString)
  val header = s"FOLDER\tAUTHORID\tPRECISION\tRECALL\tF1"
  Util.writeToFileWithHeader(header, interpolatedPrecision, s"$path/msag-interpolated.tsv")

  /*bar chart*/
  var less10: Int = 0
  var btw10and20: Int = 0
  var btw20and30: Int = 0
  var btw30and40: Int = 0
  var btw40and50: Int = 0
  var btw50and60: Int = 0
  var btw60and70: Int = 0
  var btw70and80: Int = 0
  var btw80and90: Int = 0
  var btw90and100: Int = 0

  lines.foreach(l => {

    val f1: Double = l.F1
    l.F1 match {
      case x1 if f1 < 0.1 => less10 += 1
      case x2 if (0.1 <= f1 && f1 < 0.2) => btw10and20 += 1
      case x3 if (0.2 <= f1 && f1 < 0.3) => btw20and30 += 1
      case x4 if (0.3 <= f1 && f1 < 0.4) => btw30and40 += 1
      case x5 if (0.4 <= f1 && f1 < 0.5) => btw40and50 += 1
      case x6 if (0.5 <= f1 && f1 < 0.6) => btw50and60 += 1
      case x7 if (0.6 <= f1 && f1 < 0.7) => btw60and70 += 1
      case x8 if (0.7 <= f1 && f1 < 0.8) => btw70and80 += 1
      case x9 if (0.8 <= f1 && f1 < 0.9) => btw80and90 += 1
      case x10 if (0.9 <= f1 && f1 <= 1.0) => btw90and100 += 1
    }

  })

  val barChartData: String =
    s"""
       |(<10,$less10)
       |(10-20,$btw10and20)
       |(20-30,$btw20and30)
       |(30-40,$btw30and40)
       |(40-50,$btw40and50)
       |(50-60,$btw50and60)
       |(60-70,$btw60and70)
       |(70-80,$btw70and80)
       |(80-90,$btw80and90)
       |(90-100,$btw90and100)
   """.stripMargin
  print(barChartData)


}

object ResultAnalyser extends App {
  val path: String = "/Users/visenger/data/MSAG/results"
  val input: Iterator[String] = Source.fromFile(s"${path}/msag-probe.txt").getLines()
  val edges: List[String] = Source.fromFile(s"${path}/edges-stat.txt").getLines().toList.tail

  val f1Lines: List[F1Line] = input.map(F1Line.apply(_)).toList



  private val converterToEdgesStat: (String => EdgesStat) = (e) => {
    val Array(forlderNr, authorId, edgesTotal, removed) = e.split(",")
    EdgesStat(forlderNr, authorId, edgesTotal.toInt, removed.toInt)

  }

  val edgesStat: Map[String, EdgesStat] = edges.map(converterToEdgesStat).map(e => (e.authorId, e)).toMap

  //  val pathGt: Path = Paths.get(s"${path}/analyseGt.txt")
  //  val writerGt: BufferedWriter = Files.newBufferedWriter(pathGt, StandardCharsets.UTF_8)
  //
  //  val pathLs: Path = Paths.get(s"${path}/analyseLs.txt")
  //  val writerLs: BufferedWriter = Files.newBufferedWriter(pathLs, StandardCharsets.UTF_8)

  val pathMark: Path = Paths.get(s"${path}/msag-marked.tsv")
  val writerMark: BufferedWriter = Files.newBufferedWriter(pathMark, StandardCharsets.UTF_8)

  val header: String = s"FOLDER\tAUTHOR\tRECALL\tF1\tTOTALEDGES\tREMOVEDEDGES\tMARK\n"
  writerMark.write(header)
  for (f <- f1Lines) yield {
    val id = f.authorId
    if (edgesStat.contains(id)) {
      val stat: EdgesStat = edgesStat.get(id).get
      val removedEdges: Int = stat.removed

      val mark: String = removedEdges match {
        case 1 | 2 => "a"
        case 3 | 4 => "b"
        case _ => "c"
      }

      val line: String = s"${f.folderNr}\t${f.authorId}\t${f.recall}\t${f.F1}\t${stat.edgesTotal}\t${removedEdges}\t$mark\n"
      writerMark.write(line)
      //      if (f.F1 < 0.49) writerLs.write(line)
      //      else writerGt.write(line)

    }
  }
  writerMark.close()
  //  writerGt.close()
  //  writerLs.close()

}

object TesterEverything {
  def main(args: Array[String]) {
    val folderNames: List[Int] = (5099 to 6000).toList
    val testFolder: List[Int] = Random.shuffle(folderNames).take(100)
    val withIndex: List[(Int, Int)] = testFolder.zipWithIndex
    withIndex.foreach(f => {
      print(s" ${f._1}")


    })
  }
}
