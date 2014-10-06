package de.result.evaluation

import java.io.File

import com.typesafe.config.ConfigFactory
import de.data.preparation.HospTuple
import de.util.Util

import scala.io.Source

/**
 * Evaluation of the inference results on HOSP data.
 */


/*first step: is the attributes grouping*/
object HospResultsGrouper extends App {
  val config = ConfigFactory.load()
  /* eqCityH("8762", "DOWNEY", "8760", "DOWNEYtypo") */
  val dirName = config.getString("data.hosp.resultFolder")

  for (i <- 2 to 10 if i % 2 == 0) {

    val outputLines: List[String] = Source.fromFile(s"$dirName/$i/output-data-noise-hosp-1-$i.db").getLines().toList

    val groupedAtoms: Map[String, List[String]] = outputLines.groupBy(l => l.takeWhile(_ != '('))
    println("groupedAtoms = " + groupedAtoms.keySet.mkString("\n"))

    groupedAtoms.foreach(g => {
      val name = g._1
      val atoms = if (name.startsWith("eq")) deduplicateTuples(g._2, name) else g._2
      Util.writeToFile(atoms, s"$dirName/$i/$name-$i.db")
    })



    def deduplicateTuples(attrLines: List[String], attrName: String): List[String] = {

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

  }


}

object Evaluator extends App {
  val config = ConfigFactory.load()
  val dirName = config.getString("data.hosp.resultFolder")
  for (i <- 2 to 10 if i % 2 == 0) {

    val logData: List[String] = Source.fromFile(s"$dirName/$i/log-noise-hosp-1-$i.tsv").getLines().toList
    val logDataTuples: List[(Long, List[Int])] = logData.map(l => {
      val splitted: List[String] = l.split("\\t").toList
      val idx: Long = convertToLong(splitted.head)
      val attrNums: List[Int] = convertToInt(splitted.tail)
      (idx, attrNums)
    })

    for {
      attrFileName <- new File(s"$dirName/$i").listFiles().toIterator
      if attrFileName.isFile && attrFileName.getName.startsWith("eq")
    } {
      val attrName: String = attrFileName.getName.takeWhile(_ != '-')
      println("attrFileName = " + attrName)

      val attrNum: Int = HospTuple.getIdxByAttrName(attrName)

      val containsAttrNum: List[(Long, List[Int])] = logDataTuples.filter(_._2.contains(attrNum))
      val noisyIdx: List[Long] = containsAttrNum.map(_._1)
      println("noisyIdx = " + noisyIdx.size)
      //todo 1: for each attr, compute P, R and F1.

    }
    //todo 2: for each %noise create an entry
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


case class MyTuple[AttrAtom](t: Tuple2[AttrAtom, AttrAtom]) {
  override def hashCode = t._1.hashCode + t._2.hashCode

  override def equals(other: Any) = other match {
    case MyTuple((a, b)) => a.equals(t._1) && b.equals(t._2) || a.equals(t._2) && b.equals(t._1)
    case _ => false
  }
}



