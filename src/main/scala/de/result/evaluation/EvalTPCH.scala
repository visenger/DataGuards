package de.result.evaluation

import com.typesafe.config.ConfigFactory
import de.data.preparation.DataSet

/**
 * Created by visenger on 07/10/14.
 */
object EvalTPCH extends App {

  val config = ConfigFactory.load()
  val dirName = config.getString("data.tpch.resultFolder")
  val logFileName = "log-noise"

  val evaluator = new Evaluator(Some(DataSet.TPCH), dirName, logFileName)
  evaluator.runEvaluator

}

object TPCHResultsGrouper extends App {
  val config = ConfigFactory.load()
  val dirName = config.getString("data.tpch.resultFolder")
  val fileName = "output-tpch"

  val predicatesPreparator: PredicatesGrouper = new PredicatesGrouper(dirName, fileName)
  predicatesPreparator.runGrouper
}


