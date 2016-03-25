package de.data.profiling

import com.rockymadden.stringmetric.StringMetric
import com.rockymadden.stringmetric.similarity.RatcliffObershelpMetric

/**
  * Created by visenger on 24/03/16.
  */
class SimilarityProfiling(input: List[String]) {

  val default: Double = 0.0


  def computeSimilarities: List[String] = {

    //todo: analyze input strings, and depending on their structure, decide which metric to use.
    val pairs: List[(String, String)] = input.combinations(2).map(x => (x.head, x.tail.head)).toList

    val header = s"datapointX, datapointY:, jaccard:, jaroWinkler:, levenstein:, diceSorensen:, ratcliffOber:, overlap:"
    //println(header)
    val similarities: List[String] = for ((x, y) <- pairs) yield {
      val jaccard: Double = compareWithJaccard(x, y)
      val overlap: Double = compareWithOverlap(x, y)
      val jaroWinkler: Double = compareWithJaroWinkler(x, y)
      val diceSorensen: Double = compareWithDiceSorensen(x, y)
      val levenstein: Int = getLevensteinDistance(x, y)
      val ratcliffOber: Double = compareWithRatcliffObershelp(x, y)

      val row: String = s"[$x],[$y],${jaccard},${jaroWinkler},${levenstein},${diceSorensen},${ratcliffOber},${overlap}"
      // println(row)
      row
    }
    header :: similarities
  }

  private def compareWithRatcliffObershelp(x: String, y: String): Double = {
    RatcliffObershelpMetric.compare(x.toCharArray, y.toCharArray).getOrElse(default)
  }

  private def getLevensteinDistance(x: String, y: String): Int = {
    val notApplicable: Int = Int.MaxValue
    StringMetric.compareWithLevenshtein(x.toCharArray, y.toCharArray).getOrElse(notApplicable)
  }

  private def compareWithDiceSorensen(x: String, y: String): Double = {
    StringMetric.compareWithDiceSorensen(1)(x.toCharArray, y.toCharArray).getOrElse(default)
  }

  private def compareWithJaroWinkler(x: String, y: String): Double = {
    StringMetric.compareWithJaroWinkler(x.toCharArray, y.toCharArray).getOrElse(default)
  }

  private def compareWithOverlap(x: String, y: String): Double = {
    val nGram: Int = 3
    StringMetric.compareWithOverlap(nGram)(x.toCharArray, y.toCharArray).getOrElse(default)
  }

  private def compareWithJaccard(x: String, y: String): Double = {
    StringMetric.compareWithJaccard(1)(x.toCharArray, y.toCharArray).getOrElse(default)
  }


}

class CorrelationProfiling {

}

object RelaxatorProfiling {

  def onColumn(input: List[String]) = new SimilarityProfiling(input)

  def onTwoColumns(col1Name: String, col2Name: String) = new CorrelationProfiling

}
