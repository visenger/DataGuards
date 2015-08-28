package de.data.preparation

import com.typesafe.config.{ConfigFactory, Config}

import scala.io.Source

/**
 * Created by visenger on 28/08/15.
 */
object MSAGWrangler {

  val config: Config = ConfigFactory.load()

  def preparePredicates(): Unit = {
    val path: String = config.getString("data.msag.path")
    val author = "author19525FF1.txt"
    val papers = "papers19525FF1.txt"

    val authorTuples: List[String] = Source.fromFile(s"$path/$author").getLines().toList

    authorTuples.map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffil = PaperAuthorAffil(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr)
      println(paperAuthorAffil.getPredicates)
    })

    val papersByAuthor: List[String] = Source.fromFile(s"$path/$papers").getLines().toList

    val papersTuples: List[Papers] = papersByAuthor.map(t => {
      val Array(paperId, originTitle, normalTitle, publishYear, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
      val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank)
      paper
    })

    val publishYears: List[Int] = papersTuples.map(t => {
      t.publishYear
    })
    val years: Set[Int] = publishYears.toSet
    println(years)
    val cartesian: List[List[Int]] = years.toList.combinations(2).filter(l => {
      l match {
        case List(first, second) => if (Math.abs(first - second) <= 2) true else false
        case _ => false
      }
    }).toList

    val inRangePredicates: List[String] = cartesian.map(t => {
      s"""inRange("${t(0)}", "${t(1)}")"""
    })
    println(inRangePredicates)


  }


}

case class PaperAuthorAffil(paperId: String, authorId: String, affilId: String, originAffil: String, normalAffil: String, aSequenceNr: String) {
  def getPredicates: String = {
    s"""author("$paperId", "$authorId")
                                       |affiliation("$paperId", "$affilId")""".stripMargin
  }
}

case class Papers(paperId: String, originTitle: String, normalTitle: String, publishYear: Int, publishDate: String, doi: String, originVenue: String, normalizedVenue: String, jornalId: String, paperRank: String) {
  def getPredicates: String = {
    s"""publishYear("$paperId", "$publishYear")""".stripMargin
  }
}

object MSAGPlayground extends App {
  MSAGWrangler.preparePredicates()
}
