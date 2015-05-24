package de.result.evaluation

import com.typesafe.config.{ConfigFactory, Config}
import de.data.preparation.NoiseHOSP2
import de.util.StringUtil

import scala.collection.immutable.Iterable
import scala.collection.mutable
import scala.io.Source

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
  val dataSetSizes = Array(1 /*, 10, 20, 30, 40, 80, 90, 100*/)


  def runEvaluator(): Unit = {


    for {i <- 2 to 2
         j <- dataSetSizes
         if i % 2 == 0} {

      val lines = Source.fromFile(s"$resultFolder/$i/$j/results/output-data-hosp-$j-k-noise-$i.db").getLines().toList


      val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))

      val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = for (x <- groupedByAttr; if x._1.startsWith("eq")) yield {
        val tuples: List[(AttrAtom, AttrAtom)] = deduplicateTuples(x._2)
        val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
        (attrId, tuples)
      }

      val md: Map[Int, List[AttrAtom3]] = for (x <- groupedByAttr; if x._1.startsWith("should")) yield {
        val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
        val atoms = generateAttrAtoms3(x._2)
        (attrId, atoms)
      }

      val cfdMd: Map[Int, List[TupleIdValue]] = for (x <- groupedByAttr; if x._1.startsWith("new")) yield {
        val attrId: Int = NoiseHOSP2.getIdxByAttrName(x._1)
        val atoms = generateTupleIdValues(x._2)
        (attrId, atoms)
      }

      //noise logs
      val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-hosp-$j-k-noise-$i.tsv").getLines().toList

      val noiseDictionary: Map[Int, List[Int]] = logs.map(l => {
        val strs: Array[String] = l.split("\\t")
        val lineNr: Int = strs.head.toInt
        val noisyAttrs: List[Int] = StringUtil.convertToInt(strs.tail.toList)
        (lineNr, noisyAttrs)
      }).toMap

      val attrToLineDictionary: Map[Int, List[Int]] = generateAttrToLineDictionary(noiseDictionary, NoiseHOSP2.getAllAttributeIdxs())

      attrToLineDictionary.foreach(n => println( s"""${n._1}  ${n._2.mkString(" ")}"""))
      // Start evaluation
      //cfd:
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
      println(s" data size = $j; noise = $i%; task= cfd only;  precision= $precision_cfd; recall= $recall_cfd; F1 = $f1_cfd")


    }

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
  new HOSP2Evaluator().runEvaluator()
}
