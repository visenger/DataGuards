package de.result.evaluation

import com.typesafe.config.{ConfigFactory, Config}
import de.data.preparation.TPCHTuple
import de.util.Util

import scala.collection.immutable.Iterable
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
  private val resultFolder: String = config.getString("data.tpch.resultFolder")

  val dataSetSizes = Array(500 /*, 1000, 10000, 20000, 30000, 40000, 50000, 70000, 90000, 100000*/)

  def runEvaluation: Unit = {

    for {i <- 2 to 2
         j <- dataSetSizes
         if i % 2 == 0} {

      val lines: List[String] = Source.fromFile(s"$resultFolder/$i/$j/results/output-tpch-dataSize-$j-noise-$i.db").getLines().toList
      // todo: group by name

      val groupedByAttr: Map[String, List[String]] = lines.groupBy(e => e.takeWhile(_ != '('))


      val cfd: Map[Int, List[(AttrAtom, AttrAtom)]] = for (g <- groupedByAttr; if g._1.startsWith("eq")) yield {

        val atoms = deduplicateTuples(g._2)
        val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
        (attrId, atoms)
      }
      //      println("cfd")
      //      cfd.foreach(println(_))


      val md: Map[Int, List[IDTuple]] = for (g <- groupedByAttr; if g._1.startsWith("match")) yield {

        val atoms = deduplicateArray(g._2)
        val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
        (attrId, atoms)
      }

      //      println("md")
      //      md.foreach(println(_))

      val cfdAndMd: Map[Int, List[IDTuple]] = for (g <- groupedByAttr; if g._1.startsWith("match")) yield {

        val atoms = deduplicateArray(g._2)
        val attrId: Int = TPCHTuple.getIdxByAttrName(g._1)
        (attrId, atoms)
      }

      //      println("cfd and md")
      //      cfdAndMd.foreach(println(_))

      val logs: List[String] = Source.fromFile(s"$resultFolder/$i/$j/log-dataSize-$j-noise-$i.tsv").getLines().toList

      val noiseDictionary: Map[Int, List[Int]] = logs.map(l => {
        val parts: Array[String] = l.split("\\t")
        val lineId: Int = parts.head.trim.toInt
        val attrIds: List[Int] = convertToInt(parts.tail.toList)
        (lineId, attrIds)
      }).toMap

      val attrToLineTuples: Map[Int, List[Int]] = generateAttrToLinesDictionary(noiseDictionary)

      println("attrToLineTuples = " + attrToLineTuples)

      /* Start evaluation computation */

      //cfd:
      for (x <- cfd) yield {
        val attrId = x._1

        val goldStandard: List[Int] = noiseDictionary.getOrElse(attrId, List())
        // (AttrAtom(364,31-579-682-9907typo),AttrAtom(396,31-579-682-9907))
        // AttrAtom(id, value)
        val foundAtoms: List[(AttrAtom, AttrAtom)] = x._2
        //todo: check whether first oder second are in gold standard


      }


    }

  }


  private def generateAttrToLinesDictionary(noiseDictionary: Map[Int, List[Int]]): Map[Int, List[Int]] = {

    val attrToLineTuples: List[(Int, List[Int])] = for {attr <- TPCHTuple.getAllAttributeIdxs()} yield {

      val linesForAttr: Iterable[Int] = for {noise <- noiseDictionary;
                                             if noise._2.contains(attr)} yield noise._1

      (attr, linesForAttr.toList)

    }
    attrToLineTuples.toMap
  }

  def convertToInt(l: List[String]): List[Int] = {
    l.map(_.trim.toInt)
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
  new TPCHEvaluator().runEvaluation
}
