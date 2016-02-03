import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source
import scala.util.Try

/**
  * Created by visenger on 02/02/16.
  *
  * Script for converting profiled functional dependencies from the MSAG dataset:
  *
Papers.txt

        1.Paper ID
        2.Original paper title
        3.Normalized paper title
        4.Paper publish year
        5.Paper publish date
        6.Paper Document Object Identifier (DOI)
        7.Original venue name
        8.Normalized venue name
        9.Journal ID mapped to venue name
        10.Conference series ID mapped to venue name
        11.Paper rank

PaperAuthorAffiliations.txt

        1.Paper ID
        2.Author ID
        3.Affiliation ID
        4.Original affiliation name
        5.Normalized affiliation name
        6.Author sequence number

  Profiling result:
[PaperAuthorAffiliations.csv.column1, PaperAuthorAffiliations.csv.column2, PaperAuthorAffiliations.csv.column4] --> PaperAuthorAffiliations.csv.column5
[PaperAuthorAffiliations.csv.column1, PaperAuthorAffiliations.csv.column2, PaperAuthorAffiliations.csv.column6] --> PaperAuthorAffiliations.csv.column4, PaperAuthorAffiliations.csv.column5

  */

object Converter extends App {
  val config: Config = ConfigFactory.load()

  val path = config.getString("profiled.data.path")
  val paperAuthors = "PaperAuthorAffiliations"
  val papers = "Papers"


  val authorsPapersRawDependencies: List[String] = Source.fromFile(s"$path/$paperAuthors/results.txt").getLines().toList

  val papersRawDependencies: List[String] = Source.fromFile(s"$path/$papers/results.txt").getLines().toList

  val rawDependencies: List[String] = authorsPapersRawDependencies:::papersRawDependencies

  val fds: List[FunctionalDependency] = rawDependencies.map(d => FunctionalDependency.parse(d).get)

  fds foreach (println)


}


case class FunctionalDependency(lhs: LHS, rhs: RHS) {
  override def toString: String = s"[${lhs.toString}] --> [${rhs.toString}]"
}

object FunctionalDependency {
  def parse(fdstr: String): Try[FunctionalDependency] = {

    val Array(lhs, rhs) = fdstr.split("-->")
    val withoutBorders: String = lhs.filter(_.!=('[')).filter(_.!=(']'))

    val lhsParts: Array[String] = withoutBorders.split(",")
    val lhsAttributes: Array[Attribute] = convertToAttribute(lhsParts)
    val rhsParts: Array[String] = rhs.split(",")
    val rhsAttributes: Array[Attribute] = convertToAttribute(rhsParts)


    Try(FunctionalDependency(LHS(lhsAttributes: _*), RHS(rhsAttributes: _*)))
  }

  def convertToAttribute(lhsParts: Array[String]): Array[Attribute] = {
    lhsParts.map(p => Attribute(columnsDictionary(p.trim)))
  }

  def columnsDictionary(input: String): String = {
    input match {
      case "PaperAuthorAffiliations.csv.column1" => "1.Paper ID"
      case "PaperAuthorAffiliations.csv.column2" => "2.Author ID"
      case "PaperAuthorAffiliations.csv.column3" => "3.Affiliation ID"
      case "PaperAuthorAffiliations.csv.column4" => "4.Original affiliation name"
      case "PaperAuthorAffiliations.csv.column5" => "5.Normalized affiliation name"
      case "PaperAuthorAffiliations.csv.column6" => "6.Author sequence number"

      case "Papers.csv.column1" => "1.Paper ID"
      case "Papers.csv.column2" => "2.Original paper title"
      case "Papers.csv.column3" => "3.Normalized paper title"
      case "Papers.csv.column4" => "4.Paper publish year"
      case "Papers.csv.column5" => "5.Paper publish date"
      case "Papers.csv.column6" => "6.Paper Document Object Identifier (DOI)"
      case "Papers.csv.column7" => "7.Original venue name"
      case "Papers.csv.column8" => "8.Normalized venue name"
      case "Papers.csv.column9" => "9.Journal ID mapped to venue name"
      case "Papers.csv.column10" => "10.Conference series ID mapped to venue name"
      case "Papers.csv.column11" => "11.Paper rank"
      case _ => ""
    }
  }
}

case class LHS(part: Attribute*) {
  override def toString: String = part.mkString(",")
}

case class RHS(part: Attribute*) {
  override def toString: String = part.mkString(",")
}

case class Attribute(name: String) {
  override def toString: String = name
}



