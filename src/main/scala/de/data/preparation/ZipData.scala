package de.data.preparation

import com.typesafe.config.ConfigFactory

import scala.io.Source

/**
Converts ZIP data set into the Markov Logic ground predicates form;
  */
class ZipData {
  val config = ConfigFactory.load()

  def predicatesZipTSV: List[String] = {
    val zipFile = s"${config.getString("data.zip.path")}/zipcode.csv"
    val zipLines = Source.fromFile(zipFile).getLines().zipWithIndex.drop(1)

    val zipPredicates = zipLines map (l => {
      val line = l._1
      val idx = l._2
      val Array(zip, state) = line.split(",")
      val predicates =
        s"""zipZ(\"$idx\", \"$zip\")
                                   |stateZ(\"$idx\", \"$state\")
       """.stripMargin
      predicates
    })
    zipPredicates.toList
  }

  def toAlchemyPredicates: List[String]={
    val zipFile = s"${config.getString("data.zip.path")}/zipcode.csv"
    val zipLines = Source.fromFile(zipFile).getLines().zipWithIndex.drop(1)

    val zipPredicates: Iterator[String] = for (z <- zipLines) yield {
      val line = z._1
      val idx = z._2
      val Array(zip, state) = line.split(",")

      state match {
        case "AL" => { s"""zipZ($idx, $zip)
                                            |stateZ($idx, $state)
       """.stripMargin
        }
        case _ =>""
      }
    }
    zipPredicates.toList


  }

}
