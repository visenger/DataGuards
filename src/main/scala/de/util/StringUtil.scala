package de.util

/**
 * Created by visenger on 24/05/15.
 */
object StringUtil {

  def extractParameterString(a: String): String = {
    a.substring(a.indexWhere(_ == '(') + 1, a.indexWhere(_ == ')')).replace('"', ' ')
  }

  def convertToInt(l: List[String]): List[Int] = {
    l.map(_.trim.toInt)
  }

}
