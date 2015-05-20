package de.util

import java.io.{BufferedWriter, FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.google.common.collect.HashBasedTable

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Created by larysa  25.03.14
 */
object Util {
  //  def areSynonyms(arg1: String, arg2: String): Boolean = {
  //    val synonyms: mutable.Map[ISynset, util.Set[String]] = WordnetDao.INSTANCE.getSynonymWords(arg1, false).asScala
  //    val find: Boolean = synonyms.values.exists(_.contains(arg2))
  //    find
  //  }
  //
  //  def areVerbsSynonyms(arg1: String, arg2: String): Boolean = {
  //    val synonyms: mutable.Map[ISynset, util.Set[String]] = WordnetDao.INSTANCE.getSynonymVerbs(arg1, true).asScala
  //    val find: Boolean = synonyms.values.exists(_.contains(arg2))
  //    find
  //  }
  //
  //
  //  def getAllSynonymsFor(word: String): List[(String, String)] = {
  //    val synonyms: mutable.Map[ISynset, util.Set[String]] = WordnetDao.INSTANCE.getSynonymWords(word, false).asScala
  //    val allSynonymPairs: mutable.Iterable[List[(String, String)]] = synonyms map (s => {
  //      val synsets: List[String] = s._2.asScala.toList
  //      val tuples: List[(String, String)] = for (synonym <- synsets; if (synonym != word)) yield {
  //        (word, synonym)
  //      }
  //      tuples
  //    })
  //    allSynonymPairs.flatten.toSet.toList
  //  }
  //
  //  def getSynonymsFor(word: String): List[String] = {
  //    val synonyms: mutable.Map[ISynset, util.Set[String]] = WordnetDao.INSTANCE.getSynonymWords(word, false).asScala
  //    val allSynonymPairs: mutable.Iterable[List[String]] = synonyms map (s => {
  //      val synsets: List[String] = s._2.asScala.toList
  //      val synonyms: List[String] = for (synonym <- synsets; if (synonym != word)) yield {
  //        synonym
  //      }
  //      synonyms
  //    })
  //    allSynonymPairs.flatten.toList
  //  }
  //
  //
  //  def getAllSimilarNounsFor(word: String): mutable.Map[String, Float] = {
  //    val nounSimilarityDb = SimilarityDatabase.getInstance("nounSimilarity")
  //    val similar: mutable.Map[String, Float] = nounSimilarityDb.getSimilar(word).asScala
  //    if (similar.contains(word)) {
  //      similar.remove(word)
  //    }
  //    similar
  //  }
  //

  def writeToFile(db_atoms: List[String], fileName: String) {
    val path: Path = Paths.get(fileName)
    val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    val atoms = db_atoms.toSet
    atoms foreach (a => {
      writer.write(s"$a\n")
    })
    writer.close()
  }

  def writeStrToFile(str: String, fileName: String) {
    val path: Path = Paths.get(fileName)
    val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    writer.write(str)
    writer.close()
  }

  def writeToFileWithHeader(header: String, db_atoms: List[String], fileName: String) {
    val path: Path = Paths.get(fileName)
    val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    writer.write(s"$header\n")
    val atoms = db_atoms.toSet
    atoms foreach (a => {
      writer.write(s"$a\n")
    })
    writer.close()
  }

  def writeTableToFile(fileName: String, table: HashBasedTable[String, String, Double]) {
    val path: Path = Paths.get(fileName)
    val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    val keys = table.columnKeySet.asScala
    val header: String = keys.mkString("noun\t", "\t", "\n")
    writer.write(header)
    for (key <- table.rowKeySet.asScala) {
      val row = table.row(key).asScala
      val pmiArr = new ListBuffer[Double]()
      for (stmt <- keys) {
        if (row.contains(stmt))
          pmiArr += row.get(stmt).get.toDouble
        else pmiArr += 0.0
      }
      val rowString: String = pmiArr.mkString(s"$key\t", "\t", "\n")
      writer.write(rowString)
    }
    writer.close()
  }

  def normalizeGroundAtom: String => String = {
    _.replaceAll("\"", "_").replaceAll("\\(", "_").replaceAll("\\)", "_").replaceAll(",", "_")
  }

  def playSound {

    import java.net.URL
    import javax.sound.sampled._
    val url = new URL("http://mywebpages.comcast.net/jdeshon2/wave_files/jad0001a.wav")
    val audioIn = AudioSystem.getAudioInputStream(url)
    val clip = AudioSystem.getClip
    clip.open(audioIn)
    clip.start
  }


  /**
   * Loads a resource as stream. This returns either a resource in the classpath,
   * or in case no such named resource exists, from the file system.
   */
  def getStreamFromClassPathOrFile(name: String): InputStream = {
    val is: InputStream = getClass.getClassLoader.getResourceAsStream(name)
    if (is == null) {
      new FileInputStream(name)
    }
    else {
      is
    }
  }

  def round(n: Double)(scale: Int): Double = {
    BigDecimal(n).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def getRoundNumber(n: Double): Int = round(n)(0).toInt
}

object Time {
  def code(block: () => Unit) = {
    val start = System.nanoTime
    try {
      block()
    } finally {
      val end = System.nanoTime
      println("Time taken: " + (end - start) / 1.0e9)
    }
  }

}

// usage:
//  Time.code {
//    () => val newMapi = {
//      mapi.map(MyTuple.apply).toSet.toList.map((mt: MyTuple[AttrAtom]) => mt.t)
//    }
//  }