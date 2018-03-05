package net.sansa_stack.kgml.rdf.testing

import java.util

import edu.cmu.lti.jawjaw.pobj.POS
import edu.cmu.lti.jawjaw.util.Configuration
import edu.cmu.lti.lexical_db.ILexicalDatabase
import edu.cmu.lti.lexical_db.data.Concept
import edu.cmu.lti.ws4j.util._
import edu.cmu.lti.ws4j.{Relatedness, util}


object RelatednessssssCalculatorrrrrr {
//  private var c = null
//  var enableCache = false
//  var enableTrace = false
//  protected val illegalSynset = "Synset is null."
//  protected val identicalSynset = "Synsets are identical."
//  var useRootNode = false
//
//  try c = WS4JConfiguration.getInstance
//  enableCache = c.useCache
//  enableTrace = c.useTrace
//  useRootNode = true

}

abstract class RelatednessssssCalculatorrrrrr(var db: ILexicalDatabase) {
//  pathFinder = new PathFinder(db)
//  depthFinder = new DepthFinder(db)
//  protected var pathFinder: PathFinder = null
//  protected var depthFinder: DepthFinder = null
//  private val wordSimilarity = new WordSimilarityCalculator
//
//  // abstract hook method to be implemented
//  protected def calcRelatedness(synset1: Concept, synset2: Concept): Relatedness
//
//  def getPOSPairs: util.List[Array[POS]]
//
//  // template method
//  def calcRelatednessOfSynset(synset1: Concept, synset2: Concept): Relatedness = {
//    val t0 = System.currentTimeMillis
//    val r = calcRelatedness(synset1, synset2)
//    val t1 = System.currentTimeMillis
//    r.appendTrace("Process done in = " + (t1 - t0).toDouble / 1000D + " sec (cache: " + (if (Configuration.getInstance.useCache) "enabled"
//    else "disabled") + ").\n")
//    r
//  }
//
//  def calcRelatednessOfWords(word1: String, word2: String): Double = wordSimilarity.calcRelatednessOfWords(word1, word2, this)
//
//  def getSimilarityMatrix(words1: Array[String], words2: Array[String]): Array[Array[Double]] = MatrixCalculator.getSimilarityMatrix(words1, words2, this)
//
//  def getNormalizedSimilarityMatrix(words1: Array[String], words2: Array[String]): Array[Array[Double]] = MatrixCalculator.getNormalizedSimilarityMatrix(words1, words2, this)
//
//  /**
//    * @return the db
//    */
//  def getDB: ILexicalDatabase = db
}
