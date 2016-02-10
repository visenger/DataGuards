package de.data.profiling

import scala.util.Try

/**
  * Created by visenger on 10/02/16.
  */


case class FunctionalDependency(lhs: LHS, rhs: RHS) {
  override def toString: String = s"[${lhs.toString}] --> [${rhs.toString}]"

  def isNormalized: Boolean = rhs.part match {
    case Seq(x) => true
    case _ => false
  }

  def normalizeFD: List[FunctionalDependency] = {
    if (rhs.part.size > 1) {
      val normalizedFDs: List[FunctionalDependency] =
        rhs.part.map(p => FunctionalDependency(this.lhs, RHS(p))).toList
      normalizedFDs
    }
    else List(this)
  }

  def usedPredicates: Seq[String] = {
    val lhsPredicates: Seq[String] = lhs.part.map(_.name)
    val rhsPredicates: Seq[String] = rhs.part.map(_.name)
    lhsPredicates ++ rhsPredicates
  }
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
