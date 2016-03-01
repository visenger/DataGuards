package de.data.profiling

import scala.util.Try

/**
  * Created by visenger on 10/02/16.
  */

class MarkovLogicProgramm(fds: List[FunctionalDependency]) {

  def getDeclarationFromFDs(fds: List[FunctionalDependency]): String = {
    val declaration: Set[String] = fds.foldLeft(Set[String]())(extractAttributesDef)

    val jointDeclarationPart: String = declaration.mkString("\n")
    jointDeclarationPart.split("\n").toSet.filter(emptyElement).mkString("\n")

  }

  def emptyElement: (String) => Boolean = !_.isEmpty

  def getFormulasFromFDs(fds: List[FunctionalDependency]): String = {
    val formulas: List[String] = fds.foldLeft(List[String]())(extractFD)
    formulas.mkString("\n")
  }

  def createProgrammFromFDs: String = {
    val declarationPart: String = getDeclarationFromFDs(fds)
    val formulasPart: String = getFormulasFromFDs(fds)
    s"""
       |//declaration
       |$declarationPart
       |
       |//formulas
       |$formulasPart
     """.stripMargin
  }

  private def getHiddenPredicates(rhsAttribute: String): String = {
    MarkovLogicFormula.getHiddenPredicateDefinitionByAttrName(rhsAttribute)
  }

  private def getObservedPredicates: (String) => String = {
    MarkovLogicFormula.getObservedPredicateDefinitionByAttrName _
  }

  private def extractAttributesDef: (Set[String], FunctionalDependency) => Set[String] = {
    (accumulator, fd) => {
      val lhsAttributes: Seq[String] = fd.lhs.part.map(_.name)
      val observedPreds: Set[String] = lhsAttributes.map(getObservedPredicates).toSet

      val rhsAttribute: String = fd.rhs.part.head.name
      val rhsPredicate: String = getHiddenPredicates(rhsAttribute)


      val seq: Set[String] = observedPreds + rhsPredicate
      accumulator ++ seq
    }
  }

  private def extractFD: (List[String], FunctionalDependency) => List[String] = {
    (accumulator, fd) => {
      val formula: String = MarkovLogicFormula(fd).convertToFormula
      formula :: accumulator
    }
  }

}


case class MarkovLogicFormula(fd: FunctionalDependency) {

  def convertToFormula: String = {
    fd.isNormalized match {
      case true => convertNormalizedFDToFormula(fd)
      case false => normalizeAndConvertFDToFormula(fd)
    }
  }

  def emptyElement: (String) => Boolean = !_.isEmpty

  private def convertNormalizedFDToFormula(fd: FunctionalDependency): String = {
    val lhsInFormula: String = fd.lhs.part.map(attr => {
      MarkovLogicFormula.getObservedPredInFormula(attr.name)
    }).filter(emptyElement).mkString(" v ")

    val rhs: String = fd.rhs.part.head.name
    val rhsInFormula: String = MarkovLogicFormula.getHiddenPredicateInFormula(rhs)

    s"""
       |//${fd.toString}
       |1.0 $lhsInFormula v $rhsInFormula
     """.stripMargin

  }

  private def normalizeAndConvertFDToFormula(fd: FunctionalDependency): String = {
    val normalizedFD: List[FunctionalDependency] = fd.normalizeFD
    val fdsToFormulas: List[String] = normalizedFD.map(convertNormalizedFDToFormula(_))
    val formulas: String = fdsToFormulas.mkString("\n")
    formulas
  }


}

object MarkovLogicFormula {
  val idDef: String = "paperid"
  val authorIdDef: String = "authorid"
  val affilIdDef: String = "affilid"

  val idFormula: String = "pid"
  val authorIdFormula: String = "aid"
  val affilIdFormula: String = "afid"

  val affilIDPredicate: String = s"!affiliation(${idFormula}, ${affilIdFormula}1) v !affiliation(${idFormula}, ${affilIdFormula}2)"

  def parse(fd: FunctionalDependency): Try[MarkovLogicFormula] = {
    Try(MarkovLogicFormula(fd))
  }


  def parse(fdAsString: String): Try[MarkovLogicFormula] = {
    val fd: FunctionalDependency = FunctionalDependency.parse(fdAsString).get
    Try(MarkovLogicFormula(fd))
  }

  /*using Rockit syntax, where asterix (*) denotes observed predicates;*/
  def getObservedPredicateDefinitionByAttrName(attribute: String): String = {
    /*[1.Paper ID,2.Author ID] --> [3.Affiliation ID]
    *  authorid(id1, aid), authorid(id2, aid) => sameAffiliationId(id1,id2) */

    attribute match {
      //case "1.Paper ID" => "" //paper id will be used as ID for other predicates;
      case "2.Author ID" => s"*author($idDef, $authorIdDef)"
      case "3.Affiliation ID" => s"*affiliation($idDef, $affilIdDef)"
      case "4.Original affiliation name" => s"*originAffiliationName($idDef, oname)"
      case "5.Normalized affiliation name" => s"*normalAffiliationName($idDef, nname)"
      case "6.Author sequence number" => s"*authorSeqNumber($idDef, seq)"
      case "2.Original paper title" => s"*originalPaperTitle($idDef, otitle)"
      case "3.Normalized paper title" => s"*normalizedPaperTitle($idDef, ntitle)"
      case "4.Paper publish year" => s"*paperPublishYear($idDef, year)"
      case "5.Paper publish date" => s"*paperPublishDate($idDef, date)"
      case "6.Paper Document Object Identifier (DOI)" => s"*doi($idDef, docId)"
      case "7.Original venue name" => s"*originalVenue($idDef, ovenue)"
      case "8.Normalized venue name" => s"*normalizedVenue($idDef, nvenue)"
      case "9.Journal ID mapped to venue name" => s"*journalID($idDef, journalid)"
      case "10.Conference series ID mapped to venue name" => s"*conferenceSeries($idDef, conferenceid)"
      case "11.Paper rank" => s"*rank($idDef, prank)"
      case _ => ""
    }

  }

  /*[1.Paper ID,2.Author ID] --> [3.Affiliation ID]
   *  authorid(id1, aid), authorid(id2, aid) => sameAffiliationId(id1,id2) */

  def getObservedPredInFormula(attribute: String): String = {
    val authorPredicate: String = s"!author(${idFormula}1, ${authorIdFormula}1) v !author(${idFormula}2, ${authorIdFormula}2)"
    //val affilIDPredicate: String = s"!affiliation(${authorIdFormula}, ${affilIdFormula}1) v !affiliation(${authorIdFormula}, ${affilIdFormula}2)"
    attribute match {
      //case "1.Paper ID" => "" //paper id will be used as ID for other predicates;
      case "2.Author ID" => s"!author(${idFormula}1, ${authorIdFormula}) v !author(${idFormula}2, $authorIdFormula)"
      //previous case "3.Affiliation ID" => s"$authorPredicate v !affiliation(${idFormula}1, $affilIdFormula) v !affiliation(${idFormula}2, $affilIdFormula)"
      case "3.Affiliation ID" => s"!affiliation(${idFormula}1, $affilIdFormula) v !affiliation(${idFormula}2, $affilIdFormula)"
      //previous case "4.Original affiliation name" => s"$affilIDPredicate v !originAffiliationName(${affilIdFormula}1, oname) v !originAffiliationName(${affilIdFormula}2, oname)"
      case "4.Original affiliation name" => s"!originAffiliationName(${idFormula}1, oname) v !originAffiliationName(${idFormula}2, oname)"
      //previous case "5.Normalized affiliation name" => s"$affilIDPredicate v !normalAffiliationName(${affilIdFormula}1, nname) v !normalAffiliationName(${affilIdFormula}2, nname)"
      case "5.Normalized affiliation name" => s"!normalAffiliationName(${idFormula}1, nname) v !normalAffiliationName(${idFormula}2, nname)"
      case "6.Author sequence number" => s"!authorSeqNumber(${idFormula}1, seq1) v !authorSeqNumber(${idFormula}2, seq2) " // s"!authorSeqNumber(${authorIdFormula}1, seq) v !authorSeqNumber(${authorIdFormula}2, seq)"

      case "2.Original paper title" => s"!originalPaperTitle(${idFormula}1, otitle) v !originalPaperTitle(${idFormula}2, otitle)"
      case "3.Normalized paper title" => s"!normalizedPaperTitle(${idFormula}1, ntitle) v !normalizedPaperTitle(${idFormula}2, ntitle)"
      case "4.Paper publish year" => s"!paperPublishYear(${idFormula}1, year) v !paperPublishYear(${idFormula}2, year)"
      case "5.Paper publish date" => s"!paperPublishDate(${idFormula}1, date) v !paperPublishDate(${idFormula}2, date)"
      case "6.Paper Document Object Identifier (DOI)" => s"!doi(${idFormula}1, docId) v !doi(${idFormula}2, docId)"
      case "7.Original venue name" => s"!originalVenue(${idFormula}1, ovenue) v !originalVenue(${idFormula}2, ovenue)"
      case "8.Normalized venue name" => s"!normalizedVenue(${idFormula}1, nvenue) v !normalizedVenue(${idFormula}2, nvenue)"
      case "9.Journal ID mapped to venue name" => s"!journalID(${idFormula}1, journalid) v !journalID(${idFormula}2, journalid)"
      case "10.Conference series ID mapped to venue name" => s"!conferenceSeries(${idFormula}1, conferenceid) v !conferenceSeries(${idFormula}2, conferenceid)"
      case "11.Paper rank" => s"!rank(${idFormula}1, prank) v !rank(${idFormula}2, prank)"
      case _ => ""
    }
  }


  def getHiddenPredicateDefinitionByAttrName(attribute: String): String = {
    /*[1.Paper ID,2.Author ID] --> [3.Affiliation ID]
    *  authorid(id1, aid), authorid(id2, aid) => sameAffiliationId(id1,id2) */

    attribute match {
      case "2.Author ID" => s"sameAuthor($authorIdDef, $authorIdDef)"
      case "3.Affiliation ID" => s"sameAffiliation($idDef, $idDef)"
      case "4.Original affiliation name" =>
        s"""*affiliation($idDef, $affilIdDef)
            |sameOriginNames(oname, oname)""".stripMargin
      case "5.Normalized affiliation name" =>
        s"""*affiliation($idDef, $affilIdDef)
            |sameNormalNames(nname, nname)""".stripMargin
      case "6.Author sequence number" => ""
      case "2.Original paper title" => s"sameOriginTitle($idDef, $idDef)"
      case "3.Normalized paper title" => s"sameNormalizedTitle($idDef, $idDef)"
      case "4.Paper publish year" => s"samePublishYear($idDef, $idDef)"
      case "5.Paper publish date" => s"samePublishDate($idDef, $idDef)"
      case "6.Paper Document Object Identifier (DOI)" => s"sameDOI($idDef, $idDef)"
      case "7.Original venue name" => s"sameOriginalVenue($idDef, $idDef)"
      case "8.Normalized venue name" => s"sameNormalizedVenue($idDef, $idDef)"
      case "9.Journal ID mapped to venue name" => s"sameJournal($idDef, $idDef)"
      case "10.Conference series ID mapped to venue name" => s"sameConference($idDef, $idDef)"
      case "11.Paper rank" => s"sameRank($idDef, $idDef)"
      case _ => ""
    }
  }


  def getHiddenPredicateInFormula(attribute: String): String = {
    attribute match {
      //case "2.Author ID" => s"sameAuthor(${authorIdFormula}1, ${authorIdFormula}2)"
      // [1.Paper ID,2.Author ID,4.Original affiliation name] --> [5.Normalized affiliation name]
      //todo: sameAffiliation should have two hidden predicates 1.for paper id and 2.for affiliation id ??
      case "3.Affiliation ID" => s"sameAffiliation(${idFormula}1, ${idFormula}2)"

      case "4.Original affiliation name" => s"!originAffiliationName(${idFormula}1, oname1) v !originAffiliationName(${idFormula}2, oname2) v sameOriginNames(oname1, oname2)"
      case "5.Normalized affiliation name" => s"!normalAffiliationName(${idFormula}1, nname1) v !normalAffiliationName(${idFormula}2, nname2) v sameNormalNames(nname1, nname2)"
      case "6.Author sequence number" => ""
      case "2.Original paper title" => s"sameOriginTitle(${idFormula}1, ${idFormula}2)"
      case "3.Normalized paper title" => s"sameNormalizedTitle(${idFormula}1, ${idFormula}2)"
      case "4.Paper publish year" => s"samePublishYear(${idFormula}1, ${idFormula}2)"
      case "5.Paper publish date" => s"samePublishDate(${idFormula}1, ${idFormula}2)"
      case "6.Paper Document Object Identifier (DOI)" => s"sameDOI(${idFormula}1, ${idFormula}2)"
      case "7.Original venue name" => s"sameOriginalVenue(${idFormula}1, ${idFormula}2)"
      case "8.Normalized venue name" => s"sameNormalizedVenue(${idFormula}1, ${idFormula}2)"
      case "9.Journal ID mapped to venue name" => s"sameJournal(${idFormula}1, ${idFormula}2)"
      case "10.Conference series ID mapped to venue name" => s"sameConference(${idFormula}1, ${idFormula}2)"
      case "11.Paper rank" => s"sameRank(${idFormula}1, ${idFormula}2)"
      case _ => ""
    }
  }

}

