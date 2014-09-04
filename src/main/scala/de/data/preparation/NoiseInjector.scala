package de.data.preparation

/**
 * Introduces noise to the provided data sets.
 */
class NoiseInjector() {

  def instance(path: String): this.type = {
    //
    this
  }

  def noisePercentage(n: Int): this.type = {
    //
    this
  }

  def attributes(attrs: String*): this.type = {
    //
    this
  }

  def noiseSeed(seed: String): this.type = {
    //
    this
  }

  def writeTo(path: String)={
    //todo: perform the noise injection according to the introduced parameters;
    //todo: write to file
    //todo: write logs about noise: for evaluation
  }


}

object NoiseInjector {
  def definedFor = new NoiseInjector
}
