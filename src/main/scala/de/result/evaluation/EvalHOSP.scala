package de.result.evaluation

import java.io.File

import com.typesafe.config.ConfigFactory
import de.data.preparation.{DataSet, HospTuple}
import de.util.Util

import scala.io.Source

/**
 * Evaluation of the inference results on HOSP data.
 */


/*first step: is the attributes grouping*/
object HospResultsGrouper extends App {
  val config = ConfigFactory.load()
  val dirName = config.getString("data.hosp.resultFolder")
  val fileName = "output-data-noise-hosp-1"

  val predicatesPreparator: PredicatesGrouper = new PredicatesGrouper(dirName, fileName)
  predicatesPreparator.runGrouper

  //  for (i <- 2 to 10 if i % 2 == 0) {
  //
  //    val outputLines: List[String] = Source.fromFile(s"$dirName/$i/$fileName-$i.db").getLines().toList
  //
  //    val groupedAtoms: Map[String, List[String]] = outputLines.groupBy(l => l.takeWhile(_ != '('))
  //    println("groupedAtoms = " + groupedAtoms.keySet.mkString("\n"))
  //
  //    groupedAtoms.foreach(g => {
  //      val name = g._1
  //      val atoms = if (name.startsWith("eq")) deduplicateTuples(g._2, name) else g._2
  //      Util.writeToFile(atoms, s"$dirName/$i/$name-$i.db")
  //    })
  //
  //
  //  }


  //  def deduplicateTuples(attrLines: List[String], attrName: String): List[String] = {
  //
  //    val mapi: List[Tuple2[AttrAtom, AttrAtom]] = attrLines.map(a => {
  //      val innerPart: String = a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
  //      val Array(id1, val1, id2, val2) = innerPart.split(',')
  //      Tuple2(AttrAtom(id1.trim, val1.trim), AttrAtom(id2.trim, val2.trim))
  //    })
  //
  //    //magic with deduplication ;) transform the list of tuples into a set. See MyTuple implementation for the equal method
  //    val duplicatesBuffer = mapi.map(MyTuple.apply).toSet.toList.map((mt: MyTuple[AttrAtom]) => mt.t)
  //    val diff: List[(AttrAtom, AttrAtom)] = mapi.diff(duplicatesBuffer)
  //
  //    diff.map(t => {
  //      s"$attrName(${t._1.id}, ${t._1.value}, ${t._2.id}, ${t._2.value})"
  //    })
  //  }


}

object HOSPEvaluator extends App {

  val config = ConfigFactory.load()
  val dirName = config.getString("data.hosp.resultFolder")
  val logFileName = "log-noise-hosp-1"

  //  val evaluator = new Evaluator(Some(DataSet.HOSP), dirName, logFileName)
  //  evaluator.runEvaluator

  runEvaluator

  def runEvaluator = {


    import Util._
    val config = ConfigFactory.load()
    val dirName = config.getString("data.hosp.resultFolder")
    val logFileName = "log-noise-hosp-1"

    /*eqAddressH eqCityH eqConditionH eqCountryNameH eqHospitalNameH eqMeasureNameH eqPhoneNumberH eqStateH eqZipCodeH*/
    println( s"""\\multicolumn{1}{|c|}{\\%noi} & ADDR & CITY & COND & COUNT & HOSPNAME & MEASURE & PHONE & STATE & ZIP & \\multicolumn{1}{c|}{STATE} & \\multicolumn{1}{c|}{STATE}  \\\\ \\hline""")

    for (i <- 2 to 10 if i % 2 == 0) {

      val logData: List[String] = Source.fromFile(s"$dirName/$i/$logFileName-$i.tsv").getLines().toList
      val logDataTuples: List[(Long, List[Int])] = convertLogData(logData)


      var eqAddressH: Double = 0.0
      var eqCityH: Double = 0.0
      var eqConditionH: Double = 0.0
      var eqCountryNameH: Double = 0.0
      var eqHospitalNameH: Double = 0.0
      var eqMeasureNameH: Double = 0.0
      var eqPhoneNumberH: Double = 0.0
      var eqStateH: Double = 0.0
      var eqZipCodeH: Double = 0.0

      //      println(s"_______________ % noise = $i ")

      for {
        attrFileName <- new File(s"$dirName/$i").listFiles().toIterator
        if attrFileName.isFile && attrFileName.getName.startsWith("eq")
      } {
        val attrName: String = attrFileName.getName.takeWhile(_ != '-')

        val attrNum: Int = HospTuple.getIdxByAttrName(attrName)

        val containsAttrNum: List[(Long, List[Int])] = logDataTuples.filter(_._2.contains(attrNum))
        val noisyIdx: List[Long] = containsAttrNum.map(_._1)
        //println("attrFileName = " + attrName + "; attr num= " + attrNum + "; noise elements inserted = " + noisyIdx.size)

        val inferred: List[String] = Source.fromFile(attrFileName).getLines().toList

        //everything we found of form eqCityH(2459, MELROSE PARK, 2472, MELROSE PARKtypo)
        //converted into (2459, MELROSE PARK), (2472, MELROSE PARKtypo)
        val atoms: List[(AttrAtom, AttrAtom)] = convertToAttrAtoms(inferred)

        //grouping on idx Long -> List[(AttrAtom,AttrAtom)] indexes and corresponding inferred atoms.
        val structuredAtoms: List[(Long, List[(AttrAtom, AttrAtom)])] = noisyIdx.map(i => {
          val idx: String = i.toString
          val inferredVals: List[(AttrAtom, AttrAtom)] = atoms.filter(a => {
            a._1.id == idx || a._2.id == idx
          })
          i -> inferredVals
        })

        // which idx were not found
        val withEmptyList: List[(Long, List[(AttrAtom, AttrAtom)])] = structuredAtoms.filter(_._2.isEmpty)

        val tp: Int = structuredAtoms.size - withEmptyList.size
        val correct: Int = noisyIdx.size
        //        println("noisy elements inserted = " + correct + " | found elements " + tp)
        val precision = tp.toDouble / tp.toDouble
        val recall = tp.toDouble / correct.toDouble
        val f_measure = round(2 * precision * recall / (precision + recall))(4)
        //        println(s"Attribute: $attrName precision=${precision} recall= ${recall} F measure= ${f_measure} \n")

        attrName match {
          case "eqAddressH" => {
            eqAddressH = f_measure
          }
          case "eqCityH" => {
            eqCityH = f_measure
          }
          case "eqConditionH" => {
            eqConditionH = f_measure
          }
          case "eqCountryNameH" => {
            eqCountryNameH = f_measure
          }
          case "eqHospitalNameH" => {
            eqHospitalNameH = f_measure
          }
          case "eqMeasureNameH" => {
            eqMeasureNameH = f_measure
          }
          case "eqPhoneNumberH" => {
            eqPhoneNumberH = f_measure
          }
          case "eqStateH" => {
            eqStateH = f_measure
          }
          case "eqZipCodeH" => {
            eqZipCodeH = f_measure
          }
        }


      }
      //todo 2: for each %noise create an entry
      /*eqAddressH eqCityH eqConditionH eqCountryNameH eqHospitalNameH eqMeasureNameH eqPhoneNumberH eqStateH eqZipCodeH*/

      println( s"""\\multicolumn{1}{|l|}{$i}     & $eqAddressH  & $eqCityH  & $eqConditionH  & $eqCountryNameH & $eqHospitalNameH & $eqMeasureNameH & $eqPhoneNumberH  & $eqStateH  & $eqZipCodeH   & 0.4996 & $eqStateH \\\\ \\hline""")
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

//case class AttrAtom(id: String, value: String) {
//  override def equals(other: scala.Any): Boolean = other match {
//    case AttrAtom(oId, oValue) => id == oId && value == oValue
//    case _ => false
//  }
//}
//
//
//case class MyTuple[AttrAtom](t: Tuple2[AttrAtom, AttrAtom]) {
//  override def hashCode = t._1.hashCode + t._2.hashCode
//
//  override def equals(other: Any) = other match {
//    case MyTuple((a, b)) => a.equals(t._1) && b.equals(t._2) || a.equals(t._2) && b.equals(t._1)
//    case _ => false
//  }
//}



