import com.typesafe.config.{Config, ConfigFactory}
import de.data.profiling._


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


object ConverterToMarkovLogic extends App {
  val config: Config = ConfigFactory.load()

  val path = config.getString("profiled.data.path10M")
  val paperAuthors = "PaperAuthorAffiliations"
  val papers = "Papers"


  val authorsPapersRawDependencies: List[String] = Source.fromFile(s"$path/$paperAuthors/results.txt").getLines().toList

  //val papersRawDependencies: List[String] = Source.fromFile(s"$path/$papers/results.txt").getLines().toList

  val rawDependencies: List[String] = authorsPapersRawDependencies
  val fds: List[FunctionalDependency] = rawDependencies.map(d => FunctionalDependency.parse(d).get)

  println("--- normalization ---")

  val normalizedFDs: List[FunctionalDependency] = fds.flatMap(_.normalizeFD)

  normalizedFDs foreach (println)

  println("--- Markov Logic ---")

  val markovLogicProgramm: MarkovLogicProgramm = new MarkovLogicProgramm(normalizedFDs)

  val programmFromFDs: String = markovLogicProgramm.createProgrammFromFDs

  println(programmFromFDs)

//  normalizedFDs.foreach(fd => {
//    println("-------")
//    println(s"fd: ${fd.toString}")
//    val mlFormula: String = MarkovLogicFormula(fd).convertToFormula
//    println(s"formula: $mlFormula")
//    println("-------")
//
//  })

}

object Converter extends App {
  val config: Config = ConfigFactory.load()

  val path = config.getString("profiled.data.path")
  val paperAuthors = "PaperAuthorAffiliations"
  val papers = "Papers"


  val authorsPapersRawDependencies: List[String] = Source.fromFile(s"$path/$paperAuthors/results.txt").getLines().toList

  val papersRawDependencies: List[String] = Source.fromFile(s"$path/$papers/results.txt").getLines().toList

  val rawDependencies: List[String] = authorsPapersRawDependencies ::: papersRawDependencies

  val fds: List[FunctionalDependency] = rawDependencies.map(d => FunctionalDependency.parse(d).get)

  fds foreach (println)


}

object ConverterToNormalizedFD extends App {
  val config: Config = ConfigFactory.load()

  val path = config.getString("profiled.data.path10M")
  val paperAuthors = "PaperAuthorAffiliations"
  val papers = "Papers"


  val authorsPapersRawDependencies: List[String] = Source.fromFile(s"$path/$paperAuthors/results.txt").getLines().toList

  val papersRawDependencies: List[String] = Source.fromFile(s"$path/$papers/results.txt").getLines().toList

  val rawDependencies: List[String] = authorsPapersRawDependencies ::: papersRawDependencies

  val fds: List[FunctionalDependency] = rawDependencies.map(d => FunctionalDependency.parse(d).get)

  println("--- normalized ---")

  val normalizedFDs: List[FunctionalDependency] = fds.flatMap(_.normalizeFD)

  normalizedFDs foreach (println)

  println("---rhs stats---")
  val rhsToDependencies: Map[RHS, List[FunctionalDependency]] = normalizedFDs.groupBy(_.rhs)
  val rhsToRulesSize: Map[RHS, Int] = rhsToDependencies.map(r => (r._1, r._2.size))

  rhsToRulesSize foreach (println)

  println("---lhs stats---")
  private val lhsToDependencies: Map[LHS, List[FunctionalDependency]] = normalizedFDs.groupBy(_.lhs)
  private val lhsToToRulesSize: Map[LHS, Int] = lhsToDependencies.map(l => (l._1, l._2.size))

  lhsToToRulesSize foreach (println)

  private val attributesInLHS: List[(String, Int)] = normalizedFDs.map(_.lhs).flatMap(l => l.part.map(p => (p.name, 1)))
  private val attributesInLHSToCount: Map[String, Int] = attributesInLHS.groupBy(_._1).map(a => (a._1, a._2.size))

  println("---attr in lhs stats---")
  attributesInLHSToCount foreach (println)


}






