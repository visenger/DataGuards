package de.util

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.ConfigFactory

import scala.io.Source

/**
  * Creates JSON data cleaning rules for processing by NADEEF
  */


object MsagDCRulesGenerator extends App {

  val config = ConfigFactory.load()

  val dcPath: String = config.getString("dc.rules.path")
  val dcFile: String = config.getString("dc.rules.file")
  val noisyData = config.getString("dc.rules.noisy")


  val rawRules: List[String] = Source.fromFile(s"$dcPath/$dcFile").getLines().filterNot(_.isEmpty).toList


  val preparedDCRules: List[String] = for (fd <- rawRules) yield {
    val rule: String = parseDCRule(fd)
    rule
  }

  var counter = 0
  val rules: List[String] = for (rule <- preparedDCRules) yield {

    s"""|        {
        |			"name" : "FD${counter += 1; counter}",
        |            "type" : "fd",
        |            "value" : $rule
        |        }""".stripMargin
  }
  val rulesAsJson: String = rules.mkString(",\n")


  val template =
    s"""|{
        |    "source" : {
        |        "type" : "tsv",
        |        "file" : ["./msag/nadeef-experiments/$noisyData"]
        |    },
        |    "rule" : [
          $rulesAsJson
        | ]
        |}
       """.stripMargin

  val noisyId: String = config.getString("dc.rules.id")

  val path: Path = Paths.get(s"$dcPath/dc-$noisyId.json")
  val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  writer.write(template)
  writer.close()


  def parseDCRule(input: String): String = {
    val Array(lhs, rhs) = input.split("-->")
    val lhsFinished: String = prepareDCRulePart(lhs)
    val rhsFinished: String = prepareDCRulePart(rhs)

    val dsRule: String = Seq(lhsFinished, rhsFinished).mkString( s""" [" """, "|", s""" "] """)

    dsRule
  }

  def prepareDCRulePart(input: String): String = {
    val lhsWithoutBorders: String = removeBorder(input)
    val lhsSplitted: Array[String] = removeRulesNumbers(lhsWithoutBorders)
    val lhsFinished: String = lhsSplitted.mkString(",")
    lhsFinished
  }

  def removeRulesNumbers(input: String): Array[String] = {
    input.split(",").map(_.replaceAll("\\d+\\.".r.toString(), ""))
  }

  private def removeBorder(input: String): String = {
    input.filter(_.!=('[')).filter(_.!=(']'))
  }
}
