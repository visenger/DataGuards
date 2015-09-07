package de.data.preparation

import com.typesafe.config.{Config, ConfigFactory}
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by visenger on 28/08/15.
 */
object MSAGWrangler {

  val config: Config = ConfigFactory.load()

  def preparePredicates(): Unit = {
    //val path: String = config.getString("data.msag.path")
    val path: String = "file:///home/larysa/rockit/ms-academic-graph/MicrosoftAcademicGraph"
    //val author = "author19525FF1.txt"
    val author = "PaperAuthorAffiliations.txt"
    //val papers = "papers19525FF1.txt"
    val papers = "Papers.txt"

    //val conf = new SparkConf().setMaster("local[4]").setAppName("MSAG")
    val conf = new SparkConf().setAppName("MSAG")
    conf.set("spark.storage.memoryFraction", "0.9")
    val sc = new SparkContext(conf)

    val authorTuples = sc.textFile(s"$path/$author").map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffil = PaperAuthorAffil(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr)
      paperAuthorAffil
    })

    val papersByAuthor = sc.textFile(s"$path/$papers").map(t => {
      val Array(paperId, originTitle, normalTitle, publishYear, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
      val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank)
      paper
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    authorTuples.registerTempTable("authors")
    papersByAuthor.registerTempTable("papers")

    /* joining authors and papers in order to obtain the publishing year and date of each publication */
    val query = sqlContext.sql(
      s"""SELECT a.paperId, a.authorId, a.affilId, a.originAffil, a.normalAffil, a.aSequenceNr, p.publishYear, p.publishDate
         |FROM authors a, papers p
         |WHERE a.paperId=p.paperId
       """.stripMargin)

    /* the structure of Row: todo: if the select statement changes, change the rows idx accordingly!
    r(0) --> a.paperId,
    r(1) --> a.authorId,
    r(2) --> a.affilId,
    r(3) --> a.originAffil,
    r(4) --> a.normalAffil,
    r(5) --> a.aSequenceNr,
    r(6) --> p.publishYear,
    r(7) --> p.publishDate
    * */

    /* Row corresponds to a.paperId, a.authorId, a.affilId, a.originAffil, a.normalAffil, a.aSequenceNr, p.publishYear, p.publishDate
       let's filter those rows, which do have an affiliation id */
    val filteredNotNull: RDD[Row] =
      query.map(r => Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))).filter(r => r.getString(2) != "")

    val groupedByAuthor: RDD[(Any, Iterable[Row])] = filteredNotNull.groupBy(r => r(1))

    val authorsWithManyPubs = groupedByAuthor.filter(g => {
      // who wrote more than 4 publications at the same organisation/affiliation
      val groupedByAffilId = g._2.groupBy(r => r(2))
      val publications = groupedByAffilId.filter(p => p._2.size > 4)
      publications.nonEmpty
    })

    val noisyData: RDD[LogNoisyData] = authorsWithManyPubs.map(a => {
      //todo: Achtung! to many thing happening here -> smells
      val authorId: String = a._1.asInstanceOf[String]
      val papersByAuthor: List[Row] = a._2.toList /* clean data */
      val groupedByAffilId: Map[Any, List[Row]] = papersByAuthor.groupBy(r => r(2)) /* r(2) is the AffiliationID column*/
      val manyPubs: Map[Any, List[Row]] = groupedByAffilId.filter(p => p._2.size >= 3) /* if where more than 3 publications made by the same affiliation*/
      val rowsToBeRemoved: List[Row] = manyPubs.map(p => p._2.toList.head).toList /* let's remember the first and then use it for the data cleaning*/

      /* let's remove affilId and affilNames from the clean data */
      val dirtyRows: List[Row] = rowsToBeRemoved.map(r => Row(r(0), r(1), "", "", "", r(5), r(6), r(7)))
      val cleanRowsDelta: List[Row] = papersByAuthor.diff(rowsToBeRemoved)
      val dirtyDataSet: List[Row] = cleanRowsDelta ::: dirtyRows

      LogNoisyData(authorId, convertRows(papersByAuthor), convertRows(dirtyDataSet), convertRows(rowsToBeRemoved))

    })


    /* write to disc: */



    import sys.process._
    val pathForData = "/home/larysa/rockit/ms-academic-graph/MicrosoftAcademicGraph/data"
    noisyData.collect().foreach(d => {

      val id: String = d.authorId /* create folder with this id */

      s"mkdir $pathForData/$id".!

      val cleanRows: List[String] = d.cleanData.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(cleanRows, s"$pathForData/$id/clean-$id.csv")

      val goldStnd: List[String] = d.goldStandard.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(goldStnd, s"$pathForData/$id/goldstnd-$id.csv")

      val dataWithMissingValues: List[PaperAuthorAffilRow] = d.dataWithMissingVals

      val noisyRows: List[String] = dataWithMissingValues.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(noisyRows, s"$pathForData/$id/noisy-$id.csv")

      val predicatesForMissingVals: List[String] = dataWithMissingValues.map(_.getPredicates) /* write to disc: */
      val inRangePredicates: List[String] = d.createInRangePredicates /* write to disc: */
      Util.writeToFile(predicatesForMissingVals ::: inRangePredicates, s"$pathForData/$id/predicates-$id.db")


    })
    /* todo: remove this */

    //noisyData.collect().foreach(n => println(n.dataWithMissingVals))
  }

  val convertRows: (List[Row] => List[PaperAuthorAffilRow]) = (rows) => {
    rows.map(r => PaperAuthorAffilRow(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getInt(6), r.getString(7)))
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

case class PaperAuthorAffilRow(paperId: String, authorId: String, affilId: String, originAffil: String, normalAffil: String, aSequenceNr: String, publishYear: Int, publishDate: String) {
  def getPredicates: String = {
    val affiliationPredicate: String = if (affilId == "") "" else s"""\naffiliation("$paperId", "$affilId")"""
    s"""author("$paperId", "$authorId")$affiliationPredicate
        |publishYear("$paperId", "$publishYear")""".stripMargin
  }

  def getCSVRow: String = {
    Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear, publishDate).mkString(",")
  }
}

case class LogNoisyData(authorId: String, cleanData: List[PaperAuthorAffilRow], dataWithMissingVals: List[PaperAuthorAffilRow], goldStandard: List[PaperAuthorAffilRow]) {
  /*creating inRange(pubYear, pubYear) predicates*/
  def createInRangePredicates: List[String] = {
    val years: Set[Int] = cleanData.map(d => d.publishYear).toSet

    val cartesian: List[List[Int]] = years.toList.combinations(2).filter {
      case List(first, second) => if (Math.abs(first - second) <= 2) true else false
      case _ => false
    }.toList

    val inRangePredicates: List[String] = cartesian.map(t => {
      s"""inRange("${t(0)}", "${t(1)}")"""
    })

    inRangePredicates
  }
}

object MSAGPlayground {
  def main(args: Array[String]) {
    MSAGWrangler.preparePredicates()
  }

}
