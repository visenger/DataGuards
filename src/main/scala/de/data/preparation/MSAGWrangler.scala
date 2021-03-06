package de.data.preparation

import com.typesafe.config.{Config, ConfigFactory}
import de.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by visenger on 28/08/15.
  */
object MSAGWrangler {

  val config: Config = ConfigFactory.load()

  def preparePredicates(): Unit = {

    //    val conf = new SparkConf().setAppName("MSAG")
    //    val path: String = "file:///home/larysa/rockit/ms-academic-graph/MicrosoftAcademicGraph"
    //    val author = "PaperAuthorAffiliations.txt"
    //    val papers = "Papers.txt"


    val path: String = config.getString("data.msag.path")
    val author = "author19525FF1.txt"
    val papers = "papers19525FF1.txt"
    val conf = new SparkConf().setMaster("local[4]").setAppName("MSAG")

    conf.set("spark.storage.memoryFraction", "0.9")
    val sc = new SparkContext(conf)

    val value: RDD[PaperAuthorAffil] = sc.textFile(s"$path/$author").map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffil = PaperAuthorAffil(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr)
      paperAuthorAffil
    })
    val authorTuples = value

    val papersByAuthor = sc.textFile(s"$path/$papers").map(t => {
      val Array(paperId, originTitle, normalTitle, publishYear, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank) = t.split("\\t")
      val paper = Papers(paperId, originTitle, normalTitle, publishYear.toInt, publishDate, doi, originVenue, normalizedVenue, jornalId, paperRank)
      paper
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.createSchemaRDD
    import sqlContext.implicits._

    authorTuples.toDF().registerTempTable("authors")
    papersByAuthor.toDF().registerTempTable("papers")

    val query = sqlContext.sql(
      s"""SELECT a.paperId, a.authorId, a.affilId, a.originAffil, a.normalAffil, a.aSequenceNr, p.publishYear, p.publishDate
         |FROM authors a
         |JOIN papers p ON a.paperId=p.paperId
         |WHERE a.affilId IS NOT NULL AND a.affilId <> ''
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
    //    val filteredNotNull: RDD[Row] =
    //      query.map(r => Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))).filter(r => r.getString(2) != "")

    val groupedByAuthor /*: RDD[(Any, Iterable[Row])]*/ = query.collect().groupBy(r => r(1))

    val authorsWithManyPubs = groupedByAuthor.filter(g => {
      // who wrote more than 10 publications at the same organisation/affiliation
      val groupedByAffilId = g._2.groupBy(r => r(2))
      val publications = groupedByAffilId.filter(p => p._2.size > 5)
      publications.nonEmpty
    })

    //todo: use this sample when running on cluster;
    //val sampleAuthors: RDD[(Any, Iterable[Row])] = authorsWithManyPubs.sample(false, 0.05, System.currentTimeMillis())

    val noisyData /*: RDD[LogNoisyData]*/ = authorsWithManyPubs.map(a => {
      //todo: Achtung! to many thing happening here -> smells
      val authorId: String = a._1.asInstanceOf[String]
      val papersByAuthor: List[Row] = a._2.toList
      /* clean data */
      val groupedByAffilId: Map[Any, List[Row]] = papersByAuthor.groupBy(r => r(2)) /* r(2) is the AffiliationID column*/

      val manyPubs: Map[Any, List[Row]] = groupedByAffilId.filter(p => p._2.size >= 3)
      /* if where more than 3 affiliations in portfolio*/
      val goldStandard: List[(Row, List[Row])] = manyPubs.map(p => (p._2.head, p._2.tail)).toList
      val rowsToBeRemoved: List[Row] = goldStandard.map(g => g._1)
      //manyPubs.map(p => p._2.toList.head).toList /* let's remember the first and then use it for the data cleaning*/
      //manyPubs.map(p => p._2.toList.head).toList the tail of the list is going to be a gold standard predicates.
      // that means: rowsToBeRemoved should point to the tail.

      /* let's remove affilId and affilNames from the clean data */
      val dirtyRows: List[Row] = rowsToBeRemoved.map(r => Row(r(0), r(1), "", "", "", r(5), r(6), r(7)))
      val cleanRowsDelta: List[Row] = papersByAuthor.diff(rowsToBeRemoved)
      val dirtyDataSet: List[Row] = cleanRowsDelta ::: dirtyRows

      val goldStndConverted: List[(PaperAuthorAffilRow, List[PaperAuthorAffilRow])] = goldStandard.map(g => (convertRow(g._1), convertRows(g._2)))

      //LogNoisyData(authorId, convertRows(papersByAuthor), convertRows(dirtyDataSet), convertRows(rowsToBeRemoved))

      LogNoisyData(authorId, convertRows(papersByAuthor), convertRows(dirtyDataSet), goldStndConverted)

    })


    /* write to disc: */


    import sys.process._
    // val pathForData = "/home/larysa/rockit/ms-academic-graph/MicrosoftAcademicGraph/data-sample"
    val pathForData = path

    val withIndex /*: RDD[(LogNoisyData, Long)]*/ = noisyData.zipWithIndex

    //    val count: Long = withIndex.count()
    //    println("count = " + count)

    withIndex.foreach(t => {

      val idx = t._2

      val d: LogNoisyData = t._1
      s"mkdir $pathForData/$idx".!

      //      data.foreach(d => {
      val id: String = d.authorId /* create folder with this id */

      s"mkdir $pathForData/$idx/$id".!

      val cleanRows: List[String] = d.cleanData.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(cleanRows, s"$pathForData/$idx/$id/clean-$id.csv")

      val goldStnd: List[String] = d.goldStandard.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(goldStnd, s"$pathForData/$idx/$id/goldstnd-$id.csv")

      val dataWithMissingValues: List[PaperAuthorAffilRow] = d.dataWithMissingVals

      val noisyRows: List[String] = dataWithMissingValues.map(_.getCSVRow) /* write to disc: */
      Util.writeToFile(noisyRows, s"$pathForData/$idx/$id/noisy-$id.csv")

      val predicatesForMissingVals: List[String] = dataWithMissingValues.map(_.getPredicates)
      /* write to disc: */
      val inRangePredicates: List[String] = d.createInRangePredicates /* write to disc: */
      Util.writeToFile(predicatesForMissingVals ::: inRangePredicates, s"$pathForData/$idx/$id/predicates-$id.db")

      /* evaluation related data */
      val dataForEvaluation: EvaluatorForPredicates = d.generateDataForEvaluation
      val regexToSearchResult: List[String] = dataForEvaluation.regexToSearchResult
      Util.writeToFile(regexToSearchResult, s"$pathForData/$idx/$id/regex-$id.txt")

      val referencePredicates: List[String] = dataForEvaluation.referencePredicates
      Util.writeToFile(referencePredicates, s"$pathForData/$idx/$id/reference-$id.txt")
    })

    sc.stop()
  }

  //testing only
  /*def preparePredicatesLocally(): Unit = {

    val path: String = config.getString("data.msag.path")
    val table = "clean-14222CBD.csv"

    val conf = new SparkConf().setMaster("local[4]").setAppName("MSAG")

    conf.set("spark.storage.memoryFraction", "0.9")
    val sc = new SparkContext(conf)

    //00D5078A	14222CBD	027B658D	Ege University|||Universite Paul Sabatier	ege university	1	2013	2013/07/04
    val authorTuples = sc.textFile(s"$path/$table").map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear, publishDate) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffilPubYear = PaperAuthorAffilPubYear(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear.toInt, publishDate)
      paperAuthorAffil
    })



    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    authorTuples.registerTempTable("authors")


    val query = sqlContext.sql(
      s"""SELECT a.paperId, a.authorId, a.affilId, a.originAffil, a.normalAffil, a.aSequenceNr, a.publishYear, a.publishDate
          |FROM authors a
          |WHERE a.affilId IS NOT NULL AND a.affilId <> ''
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


    val groupedByAuthor: RDD[(Any, Iterable[Row])] = query.groupBy(r => r(1))

    val authorsWithManyPubs = groupedByAuthor.filter(g => {
      // who wrote more than 10 publications at the same organisation/affiliation
      val groupedByAffilId = g._2.groupBy(r => r(2))
      val publications = groupedByAffilId.filter(p => p._2.size > 5)
      publications.nonEmpty
    })


    val noisyData: RDD[LogNoisyData] = authorsWithManyPubs.map(a => {
      //todo: Achtung! to many thing happening here -> smells
      val authorId: String = a._1.asInstanceOf[String]
      val papersByAuthor: List[Row] = a._2.toList /* clean data */
      val groupedByAffilId: Map[Any, List[Row]] = papersByAuthor.groupBy(r => r(2)) /* r(2) is the AffiliationID column*/

      val manyPubs: Map[Any, List[Row]] = groupedByAffilId.filter(p => p._2.size >= 3) /* if where more than 3 affiliations in portfolio*/
      val goldStandard: List[(Row, List[Row])] = manyPubs.map(p => (p._2.head, p._2.tail)).toList
      val rowsToBeRemoved: List[Row] = goldStandard.map(g => g._1)
      //manyPubs.map(p => p._2.toList.head).toList /* let's remember the first and then use it for the data cleaning*/
      //manyPubs.map(p => p._2.toList.head).toList the tail of the list is going to be a gold standard predicates.
      // that means: rowsToBeRemoved should point to the tail.

      /* let's remove affilId and affilNames from the clean data */
      val dirtyRows: List[Row] = rowsToBeRemoved.map(r => Row(r(0), r(1), "", "", "", r(5), r(6), r(7)))
      val cleanRowsDelta: List[Row] = papersByAuthor.diff(rowsToBeRemoved)
      val dirtyDataSet: List[Row] = cleanRowsDelta ::: dirtyRows

      val goldStndConverted: List[(PaperAuthorAffilRow, List[PaperAuthorAffilRow])] = goldStandard.map(g => (convertRow(g._1), convertRows(g._2)))

      //LogNoisyData(authorId, convertRows(papersByAuthor), convertRows(dirtyDataSet), convertRows(rowsToBeRemoved))

      LogNoisyData(authorId, convertRows(papersByAuthor), convertRows(dirtyDataSet), goldStndConverted)

    })


    /* write to disc: */


    import sys.process._
    // val pathForData = "/home/larysa/rockit/ms-academic-graph/MicrosoftAcademicGraph/data-sample"
    val pathForData = s"$path/local"

    val withIndex: RDD[(LogNoisyData, Long)] = noisyData.zipWithIndex

    //    val count: Long = withIndex.count()
    //    println("count = " + count)

    withIndex.foreach(t => {

      val idx = t._2

      val d: LogNoisyData = t._1
      s"mkdir $pathForData/$idx".!

      //      data.foreach(d => {
      val id: String = d.authorId /* create folder with this id */

      s"mkdir $pathForData/$idx/$id".!

      val cleanRows: List[String] = d.cleanData.map(_.getTSVRow) /* write to disc: */
      Util.writeToFile(cleanRows, s"$pathForData/$idx/$id/clean-$id.tsv")

      val goldStnd: List[String] = d.goldStandard.map(_.getTSVRow) /* write to disc: */
      Util.writeToFile(goldStnd, s"$pathForData/$idx/$id/goldstnd-$id.tsv")

      val dataWithMissingValues: List[PaperAuthorAffilRow] = d.dataWithMissingVals

      val noisyRows: List[String] = dataWithMissingValues.map(_.getTSVRow) /* write to disc: */
      Util.writeToFile(noisyRows, s"$pathForData/$idx/$id/noisy-$id.tsv")

      val predicatesForMissingVals: List[String] = dataWithMissingValues.map(_.getPredicates) /* write to disc: */
      val inRangePredicates: List[String] = d.createInRangePredicates /* write to disc: */
      Util.writeToFile(predicatesForMissingVals ::: inRangePredicates, s"$pathForData/$idx/$id/predicates-$id.db")

      /* evaluation related data */
      val dataForEvaluation: EvaluatorForPredicates = d.generateDataForEvaluation
      val regexToSearchResult: List[String] = dataForEvaluation.regexToSearchResult
      Util.writeToFile(regexToSearchResult, s"$pathForData/$idx/$id/regex-$id.txt")

      val referencePredicates: List[String] = dataForEvaluation.referencePredicates
      Util.writeToFile(referencePredicates, s"$pathForData/$idx/$id/reference-$id.txt")
    })

    sc.stop()

  }*/

  val convertRows: (List[Row] => List[PaperAuthorAffilRow]) = (rows) => {
    rows.map(r => PaperAuthorAffilRow(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getInt(6), r.getString(7)))
  }

  val convertRow: (Row => PaperAuthorAffilRow) = (r) => PaperAuthorAffilRow(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getInt(6), r.getString(7))

}

case class PaperAuthorAffil(paperId: String, authorId: String, affilId: String, originAffil: String, normalAffil: String, aSequenceNr: String) {
  def getPredicates: String = {
    s"""author("$paperId", "$authorId")
       |affiliation("$paperId", "$affilId")
       |authorSeqNumber("$authorId", "$aSequenceNr")""".stripMargin
  }
}

case class PaperAuthorAffilPubYear(paperId: String, authorId: String, affilId: String, originAffil: String, normalAffil: String, aSequenceNr: String, publishYear: Int, publishDate: String) {
  def getPredicates: String = {
    s"""author("$paperId", "$authorId")
       |affiliation("$paperId", "$affilId")
       |authorSeqNumber("$authorId", "$aSequenceNr")
       |publishYear("$paperId", "$publishYear")""".stripMargin
  }
}


case class Papers(paperId: String, originTitle: String, normalTitle: String, publishYear: Int, publishDate: String, doi: String, originVenue: String, normalizedVenue: String, jornalId: String, paperRank: String) {
  def getPredicates: String = {
    s"""publishYear("$paperId", "$publishYear")""".stripMargin
  }
}

case class PaperAuthorAffilRow(paperId: String, authorId: String, affilId: String, originAffil: String, normalAffil: String, aSequenceNr: String, publishYear: Int, publishDate: String) {

  import Util._

  def getPredicates: String = {
    val affiliationPredicate: String = if (affilId == "") ""
    else
      s"""\naffiliation("$paperId", "${normalizeGroundAtom(affilId)}")
         |authorSeqNumber("$authorId", "$aSequenceNr")
         |originAffiliationName("$affilId","${normalizeGroundAtom(originAffil)}")
         |normalAffiliationName("$affilId","${normalizeGroundAtom(normalAffil)}")""".stripMargin
    s"""author("$paperId", "$authorId")$affiliationPredicate
       |publishYear("$paperId", "$publishYear")""".stripMargin
  }

  def getCSVRow: String = {
    Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear, publishDate).mkString(",")
  }

  def getTSVRow: String = {
    Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear, publishDate).mkString("\t")
  }
}

case class EvaluatorForPredicates(removedRows: List[PaperAuthorAffilRow],
                                  regexToSearchResult: List[String],
                                  referencePredicates: List[String]) {

}

case class LogNoisyData(authorId: String,
                        cleanData: List[PaperAuthorAffilRow],
                        dataWithMissingVals: List[PaperAuthorAffilRow],
                        goldStndConverted: List[(PaperAuthorAffilRow, List[PaperAuthorAffilRow])]) {
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

  /*
  sameAffiliation(paperid, paperid)
  sameOriginNames(oname, oname)
  sameOriginNamesByPaperId(paperid, paperid)
  missingOriginName(paperid, oname)
  sameNormalNames(nname, nname)
  sameNormalNamesByPaperId(paperid, paperid)
  * */
  def generateDataForEvaluation: EvaluatorForPredicates = {
    val evaluatorForPredicateses: List[EvaluatorForPredicates] = goldStndConverted.map(g => {
      val removedRow: PaperAuthorAffilRow = g._1
      val paperid: String = removedRow.paperId
      val originName: String = Util.normalizeGroundAtom(removedRow.originAffil)
      val normalName: String = Util.normalizeGroundAtom(removedRow.normalAffil)

      // generate regex for every predicate e.g: sameAffiliation\\(\\".+\\", "$paperid"\\)
      val re11 =
        s"""sameAffiliation\\(\\".+\\", "$paperid"\\)"""
      val re12 = s"""sameAffiliation\\("$paperid", \\".+\\"\\)"""

      val re21 = s"""sameOriginNames\\(\\".+\\", "$originName"\\)"""
      val re22 = s"""sameOriginNames\\("$originName", \\".+\\"\\)"""

      val re31 = s"""sameNormalNames\\(\\".+\\", "$normalName"\\)"""
      val re32 = s"""sameNormalNames\\("$normalName", \\".+\\"\\)"""

      val references: List[PaperAuthorAffilRow] = g._2
      val referencePredicates: List[String] = references.map(ref => {
        s"""sameAffiliation("$paperid", "${ref.paperId}")
           |sameOriginNames("$originName", "${Util.normalizeGroundAtom(ref.originAffil)}")
           |sameNormalNames("$normalName", "${Util.normalizeGroundAtom(ref.normalAffil)}")""".stripMargin
      })
      EvaluatorForPredicates(List(removedRow), List(re11, re12, re21, re22, re31, re32), referencePredicates)

    })

    val evaluator: EvaluatorForPredicates = evaluatorForPredicateses.reduceLeft((x, y) =>
      new EvaluatorForPredicates(x.removedRows ::: y.removedRows,
        x.regexToSearchResult ::: y.regexToSearchResult,
        x.referencePredicates ::: y.referencePredicates))

    evaluator
  }

  /**
    *
    * @return only lines, which were removed from the data
    */
  def goldStandard = goldStndConverted.map(_._1)


}


object MSAGPlayground {
  def main(args: Array[String]) {
    MSAGWrangler.preparePredicates()
  }

}

object LocalMSAGPlayground {
  def main(args: Array[String]) {
    // MSAGWrangler.preparePredicatesLocally()


    /*val config: Config = ConfigFactory.load()
    val path: String = config.getString("data.msag.path")
    val table = "clean-14222CBD.csv"
    val source: Iterator[String] = Source.fromFile(s"$path/$table").getLines()
    val authorTuples = source.map(t => {
      val Array(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear, publishDate) = t.split("\\t")
      val paperAuthorAffil: PaperAuthorAffilPubYear = PaperAuthorAffilPubYear(paperId, authorId, affilId, originAffil, normalAffil, aSequenceNr, publishYear.toInt, publishDate)

      paperAuthorAffil
    })

    authorTuples foreach( println(_))*/


  }

}
