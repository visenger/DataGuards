package de.result.evaluation

import com.typesafe.config.{ConfigFactory, Config}

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
      lines.foreach(println(_))

    }

  }

}

object PlaygroundEvaluator extends App {
  new TPCHEvaluator().runEvaluation
}
