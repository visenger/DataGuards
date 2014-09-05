package de.data.preparation

/**
 * Introduces noise to the provided data sets.
 */
class NoiseInjector() {

  import Seed._
  import DataSet._

  var seedType = None: Option[Seed]
  var dataset = None: Option[DataSet]
  var dataPath: String = ""
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

  def attributes(attrs: String*): this.type = {
    //
    this
  }

  def noiseSeed(seed: Seed): this.type = {
    seedType = Some(seed)
    this
  }

  def writeTo(path: String) = {

    dataset match {
      case Some(TPCH) => TpchNoiseInjector.definedFor
      case Some(HOSP) => HospNoiseInjector.definedFor
      case _ => ???
    }
  }


}

object NoiseInjector {
  def definedFor = new NoiseInjector
}


object Seed extends Enumeration {
  type Seed = Value
  val NUM, TEXT = Value
}

object DataSet extends Enumeration {
  type DataSet = Value
  val HOSP, TPCH = Value
}


class HospNoiseInjector {

  //todo: perform the noise injection according to the introduced parameters;
  //todo: write to file
  //todo: write logs about noise: for evaluation

}

object HospNoiseInjector {
  def definedFor = new HospNoiseInjector
}

class TpchNoiseInjector {

  //todo: perform the noise injection according to the introduced parameters;
  //todo: write to file
  //todo: write logs about noise: for evaluation

}

object TpchNoiseInjector {
  def definedFor = new TpchNoiseInjector
}
