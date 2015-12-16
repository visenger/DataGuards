package de.markov.logic.grounding

import com.typesafe.config.ConfigFactory
import de.data.preparation.{Papers, PaperAuthorAffil}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by visenger on 02/12/15.
  */
class LargeScaleGrounder {

}

object SparkGrounder {

  val config = ConfigFactory.load()

  def ground: Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("MLNGROUND")
    val sc = new SparkContext(sparkConf)

    val path: String = config.getString("data.msag.path")
    val author = "author19525FF1.txt"
    val papers = "papers19525FF1.txt"

    val authorTuples = sc.textFile(s"$path/$author").map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffil = PaperAuthorAffil(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr)
      paperAuthorAffil
    })

    val papersByAuthor = sc.textFile(s"$path/$papers").map(t => {
      val Array(paperId, originTitle, normalTitle, publishYear, publishDate,
      doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
      val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate,
        doi, originVenue, normalizedVenue, jornalId, paperRank)
      paper
    })

    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    import sqlContext.createSchemaRDD
    //
    //    authorTuples.registerTempTable("authors")
    //    papersByAuthor.registerTempTable("papers")

    /*
    *

*publishYear(paperid, pubyear)
*author(paperid, authorid)
*affiliation(paperid, affilid)
*inRange(pubyear, pubyear)
*originAffiliationName(affilid, oname)
*normalAffiliationName(affilid, nname)

sameAffiliation(paperid, paperid)
sameOriginNames(oname, oname)
sameOriginNamesByPaperId(paperid, paperid)
missingOriginName(paperid, oname)
sameNormalNames(nname, nname)
sameNormalNamesByPaperId(paperid, paperid)
    * */

  }
}
