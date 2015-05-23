package de.result.evaluation

import com.typesafe.config.{ConfigFactory, Config}

/**
 * Created by visenger on 23/05/15.
 *
 * cdf:
 * eqHospitalNameH(hid, name, hid, name)
eqAddressH(hid, address, hid, address)
eqCityH(hid, city, hid, city)
eqStateH(hid, state, hid, state)
eqZipCodeH(hid, code, hid, code)
eqCountryNameH(hid, country, hid, country)
eqPhoneNumberH(hid, number, hid, number)
eqMeasureNameH(hid, measurename, hid, measurename)
eqConditionH(hid, condition, hid, condition)

 eqStateH(hid, state, hid, state)

 md:
shouldBeStateH(hid, state, state)

 cfd and md interleaved:
newStateH(hid, state)

 */
class HOSP2Evaluator() {

  val config: Config = ConfigFactory.load()
  private val resultFolder: String = config.getString("data.hosp.resultFolder")

  def runEvaluator(): Unit = {
  
  }


}
