package de.result.evaluation

import java.io.File

import de.data.preparation.{TPCHTuple, HospTuple, DataSet}
import de.data.preparation.DataSet.DataSet
import de.util.Util


import scala.io.Source

/**
 * Created by visenger on 07/10/14.
 */


class PredicatesGrouper(dirName: String, fileName: String) {


  def runGrouper = {
    for (i <- 2 to 2 if i % 2 == 0) {

      val outputLines: List[String] = Source.fromFile(s"$dirName/$i/$fileName-$i.db").getLines().toList

      val groupedAtoms: Map[String, List[String]] = outputLines.groupBy(l => l.takeWhile(_ != '('))
      println("groupedAtoms = " + groupedAtoms.keySet.mkString("\n"))

      groupedAtoms.foreach(g => {
        val name = g._1
        val atoms = if (name.startsWith("eq")) deduplicateTuples(g._2, name) else deduplicateArray(g._2, name)
        Util.writeToFile(atoms, s"$dirName/$i/$name-interleaved-$i.db")
      })


    }

  }

  private def deduplicateTuples(attrLines: List[String], attrName: String): List[String] = {

    val mapi: List[Tuple2[AttrAtom, AttrAtom]] = attrLines.map(a => {
      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
      val Array(id1, val1, id2, val2) = innerPart.split(',')
      Tuple2(AttrAtom(id1.trim, val1.trim), AttrAtom(id2.trim, val2.trim))
    })

    //magic with deduplication ;) transform the list of tuples into a set. See MyTuple implementation for the equal method
    val duplicatesBuffer = mapi.map(MyTuple.apply).toSet.toList.map((mt: MyTuple[AttrAtom]) => mt.t)
    val diff: List[(AttrAtom, AttrAtom)] = mapi.diff(duplicatesBuffer)

    diff.map(t => {
      s"$attrName(${t._1.id}, ${t._1.value}, ${t._2.id}, ${t._2.value})"
    })
  }


  private def deduplicateArray(attrLines: List[String], attrName: String): List[String] = {

    val attrList: List[IDTuple] = attrLines.map(a => {
      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
      val Array(id1, id2) = innerPart.split(',')
      IDTuple(id1.trim, id2.trim)
    })
    val dedupicatedAttrs: Set[IDTuple] = attrList.toSet


    val attrs: Set[String] = dedupicatedAttrs.map(a => {
      s"$attrName(${a.id1}, ${a.id2})"
    })
    attrs.toList
  }
}


class Evaluator(var dataset: Option[DataSet] = None: Option[DataSet], dirName: String, logFileName: String) {

  def runTPCHEvaluator = {

    for (i <- 2 to 2 if i % 2 == 0) {

      val logData: List[String] = Source.fromFile(s"$dirName/$i/$logFileName-$i.tsv").getLines().toList
      val logDataTuples: List[(Long, List[Int])] = convertLogData(logData)

      println(s"_______________ % noise = $i ")

      for {
        attrFileName <- new File(s"$dirName/$i").listFiles().toIterator
        if attrFileName.isFile && attrFileName.getName.startsWith("eq") && attrFileName.getName.contains("interleaved")
      } {
        /*val attrName: String = attrFileName.getName.takeWhile(_ != '-')

        val attrNum: Int = getIdxByName(attrName)

        val containsAttrNum: List[(Long, List[Int])] = logDataTuples.filter(_._2.contains(attrNum))
        val noisyIdx: List[Long] = containsAttrNum.map(_._1)

        val inferred: List[String] = Source.fromFile(attrFileName).getLines().toList

        //everything we found of form eqCityH(2459, MELROSE PARK, 2472, MELROSE PARKtypo)
        //converted into (2459, MELROSE PARK), (2472, MELROSE PARKtypo)
        val atoms: List[(AttrAtom, AttrAtom)] = convertToAttrAtoms(inferred)

        //grouping on idx Long -> List[(AttrAtom,AttrAtom)] indexes and corresponding inferred atoms.
        val structuredAtoms: List[(Long, List[(AttrAtom, AttrAtom)])] = noisyIdx.map(i => {
          val idx: String = i.toString
          val inferredVals: List[(AttrAtom, AttrAtom)] = atoms.filter(a => {
            (a._1.id == idx || a._2.id == idx) && (a._1.value != a._2.value)
          })
          i -> inferredVals
        })

        // which idx were not found
        val withEmptyList: List[(Long, List[(AttrAtom, AttrAtom)])] = structuredAtoms.filter(_._2.isEmpty)

        val tp: Int = structuredAtoms.size - withEmptyList.size
        val correct: Int = noisyIdx.size
        println("noisy elements inserted = " + correct + " | found elements " + tp)
        val precision = tp.toDouble / tp.toDouble
        val recall = tp.toDouble / correct.toDouble
        val f_measure = 2 * precision * recall / (precision + recall)
        */

        val attrName: String = attrFileName.getName.takeWhile(_ != '-')

        val attrNum: Int = TPCHTuple.getIdxByAttrName(attrName)

        val containsAttrNum: List[(Long, List[Int])] = logDataTuples.filter(_._2.contains(attrNum))
        val noisyIdxInserted: List[Long] = containsAttrNum.map(_._1)
        //println("attrFileName = " + attrName + "; attr num= " + attrNum + "; noise elements inserted = " + noisyIdxInserted.size)

        val inferred: List[String] = Source.fromFile(attrFileName).getLines().toList

        //everything we found of form eqCityH(2459, MELROSE PARK, 2472, MELROSE PARKtypo)
        //converted into (2459, MELROSE PARK), (2472, MELROSE PARKtypo)
        val allAtomsFound: List[(AttrAtom, AttrAtom)] = convertToAttrAtoms(inferred)

        //grouping on idx Long -> List[(AttrAtom,AttrAtom)] indexes and corresponding inferred allAtomsFound.
        val whichIdxIsFound: List[(Long, List[(AttrAtom, AttrAtom)])] = noisyIdxInserted.map(i => {
          val idx: String = i.toString
          val inferredVals: List[(AttrAtom, AttrAtom)] = allAtomsFound.filter(a => {
            (a._1.id == idx || a._2.id == idx)
          })
          i -> inferredVals
        })

        // which idx were not found
        val withEmptyList: List[(Long, List[(AttrAtom, AttrAtom)])] = whichIdxIsFound.filter(_._2.isEmpty)

        // tp: selected and correct
        val selectedAndCorrect: List[(Long, List[(AttrAtom, AttrAtom)])] = whichIdxIsFound.map(e => {
          val idx = e._1
          val differentVals: List[(AttrAtom, AttrAtom)] = e._2.filter(t => {
            t._1.value != t._2.value
          })
          idx -> differentVals
        }).filter(_._2.nonEmpty)
        import de.util.Util._
        // val correct= tp + fn
        // val selected= tp + fp
        val selected: Int = whichIdxIsFound.size - withEmptyList.size
        val correct: Int = noisyIdxInserted.size
        val tp: Int = selectedAndCorrect.size //withEmptyList.size
        //
        val precision = tp.toDouble / selected.toDouble
        val recall = tp.toDouble / correct.toDouble
        //        val f_measure = round(2 * precision * recall / (precision + recall))(4)
        val f_measure = 2 * precision * recall / (precision + recall)


        println(s"Attribute: $attrName precision=${precision} recall= ${recall} F measure= ${f_measure} \n")

      }
      //todo 2: for each %noise create an entry
    }
  }


  def getIdxByName(name: String): Int = {
    dataset match {
      case Some(DataSet.HOSP) => HospTuple.getIdxByAttrName(name)
      case Some(DataSet.TPCH) => TPCHTuple.getIdxByAttrName(name)
      case None => Int.MinValue
    }
  }


  def convertLogData(logData: List[String]): List[(Long, List[Int])] = {
    val logDataTuples: List[(Long, List[Int])] = logData.map(l => {
      val splitted: List[String] = l.split("\\t").toList
      val idx: Long = convertToLong(splitted.head)
      val attrNums: List[Int] = convertToInt(splitted.tail)
      (idx, attrNums)
    })
    logDataTuples
  }

  def convertToAttrAtoms(input: List[String]): List[(AttrAtom, AttrAtom)] = {
    val mapi: List[(AttrAtom, AttrAtom)] = input.map(a => {
      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')'))
      val Array(id1, val1, id2, val2) = innerPart.split(',')
      Tuple2(AttrAtom(id1.trim, val1.trim), AttrAtom(id2.trim, val2.trim))
    })
    mapi
  }


  def convertToLong(s: String): Long = {
    s.trim.toLong
  }

  def convertToInt(l: List[String]): List[Int] = {
    l.map(_.trim.toInt)
  }

}

case class AttrAtom(id: String, value: String) {
  override def equals(other: scala.Any): Boolean = other match {
    case AttrAtom(oId, oValue) => id == oId && value == oValue
    case _ => false
  }
}

case class IDTuple(id1: String, id2: String) {


  override def hashCode(): Int = id1.hashCode + id2.hashCode

  override def equals(other: scala.Any): Boolean = other match {
    case IDTuple(oId1, oId2) => oId1.equals(id1) && oId2.equals(id2) || oId1.equals(id2) && oId2.equals(id1)
    case _ => false
  }
}


case class MyTuple[AttrAtom](t: Tuple2[AttrAtom, AttrAtom]) {
  override def hashCode = t._1.hashCode + t._2.hashCode

  override def equals(other: Any) = other match {
    case MyTuple((a, b)) => a.equals(t._1) && b.equals(t._2) || a.equals(t._2) && b.equals(t._1)
    case _ => false
  }
}