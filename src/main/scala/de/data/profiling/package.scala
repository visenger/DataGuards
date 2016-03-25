package de.data

import de.data.profiling.RelaxatorProfiling
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by visenger on 24/03/16.
  */
package object profiling {

}


//todo: return Seq of Values -> data types supported by spark
package object dataframe_util {

  implicit class TuplesExtension(val input: List[String]) {

    def computeSimilaritiesOnColumn: List[String] = {
      RelaxatorProfiling.onColumn(input).computeSimilarities
    }
  }

  implicit class DFExtension(val df: DataFrame) {
    def topK(colName: String, k: Int): DataFrame = {
      df.
        select(colName).
        filter(df(colName) !== "").
        groupBy(colName).
        count().
        orderBy(desc("count")).limit(k)
    }

    def top10(colName: String, k: Int = 10): DataFrame = {
      df.
        select(colName).
        filter(df(colName) !== "").
        groupBy(colName).
        count().
        orderBy(desc("count")).limit(k)
    }

    def stringValuesByColumn(colName: String): List[String] = {
      val rdd: RDD[Row] = df.select(colName).rdd
      val valuesAsStr: List[String] = rdd.map(r => r(0).toString).collect().toList
      valuesAsStr
    }


  }


}
