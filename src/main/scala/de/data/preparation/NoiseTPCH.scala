package de.data.preparation

/**
 * Processing TPCH data set. This class introduces noise to TPCH. Result is written to the specified folder.
 */


class TpchNoiseInjector(val datapath: String, val noisePersentage: Int = 2, val writeTo: String) {

  def inject = {
    println(s"input = $datapath; noise = $noisePersentage; result folder= $writeTo")
  }

  //todo: perform the noise injection according to the introduced parameters;
  //todo: write to file
  //todo: write logs about noise: for evaluation

}
