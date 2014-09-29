package de.data.preparation

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Introduces noise to the provided data sets.
 */
class NoiseInjector() {

  import DataSet._

  val config: Config = ConfigFactory.load()

  var dataset = None: Option[DataSet]
  var dataPath: String = ""
  var resultPath = None: Option[String]
  var noiseP: Int = 2


  def tpch(path: String): this.type = {
    dataset = Some(TPCH)
    dataPath = path
    this
  }

  def hosp(path: String): this.type = {
    dataset = Some(HOSP)
    dataPath = path
    this
  }


  def noisePercentage(n: Int): this.type = {
    noiseP = n
    this
  }


  def writeTo(path: String): this.type = {
    resultPath = Some(path)
    this
  }

  def inject: Unit = {


    dataset match {
      case Some(TPCH) => {
        val resultFolder = resultPath match {
          case Some(x) => x
          case None => config.getString("data.tpch.resultFolder")
        }
        new TpchNoiseInjector(dataPath, noiseP, resultFolder).inject
      }
      case Some(HOSP) => {
        val resultFolder = resultPath match {
          case Some(x) => x
          case None => config.getString("data.hosp.resultFolder")
        }
        new HospNoiseInjector(dataPath, noiseP, resultFolder).inject
      }
      case None => println("data is not specified.")
    }
  }


}

object NoiseInjector {
  def definedFor = new NoiseInjector
}

object DataSet extends Enumeration {
  type DataSet = Value
  val HOSP, TPCH = Value
}


object PlaygroundForNoise extends App {
  private val config: Config = ConfigFactory.load()
  NoiseInjector.definedFor.hosp(config.getString("data.hosp.path")).noisePercentage(2).writeTo(config.getString("data.hosp.resultFolder")).inject
  //  NoiseInjector.definedFor.tpch(config.getString("data.tpch.path")).noisePercentage(2).inject
}


