package net.sansa_stack.kgml.rdf

import breeze.linalg.max
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}

/**
  * Created by afshin on 26.10.17.
  */
class SimilarityHandler(initialThreshold: Double) extends java.io.Serializable {

  private var threshold = initialThreshold

  val wn = WordNet()

  // predicates can be a verb or a noun
  def getMeanWordNetPredicateSimilarity(string1: String, string2: String): Double = {

    val string1AsVerb = wn.synset(string1, POS.VERB, 1)
    val string2AsVerb = wn.synset(string2, POS.VERB, 1)
    //7 similarity measures
    val verbPathSimilarity = wn.pathSimilarity(string1AsVerb, string2AsVerb)
    val verbLchSimilarity = wn.lchSimilarity(string1AsVerb, string2AsVerb)
    val verbWupSimilarity = wn.wupSimilarity(string1AsVerb, string2AsVerb)
    val verbResSimilarity = wn.resSimilarity(string1AsVerb, string2AsVerb)
    val verbJcnSimilarity = wn.jcnSimilarity(string1AsVerb, string2AsVerb)
    val verbLinSimilarity = wn.linSimilarity(string1AsVerb, string2AsVerb)
    val verbLeskSimilarity = wn.leskSimilarity(string1AsVerb, string2AsVerb)

    val verbMeanSim = (verbPathSimilarity + verbLchSimilarity + verbWupSimilarity + verbResSimilarity + verbJcnSimilarity
      + verbLinSimilarity + verbLeskSimilarity) / 7

    val meanSim = max(this.getMeanWordNetNounSimilarity(string1, string2), verbMeanSim)
    meanSim
  }

  //
  def getMeanWordNetNounSimilarity(string1: String, string2: String): Double = {

    val string1AsNoun = wn.synset(string1, POS.NOUN, 1)
    val string2AsNoun = wn.synset(string2, POS.NOUN, 1)
    val nounPathSimilarity = wn.pathSimilarity(string1AsNoun, string2AsNoun)
    println("nounPathSimilarity = "+nounPathSimilarity)

    var nounMeanSim = 0.0
    if (nounPathSimilarity.equals(1.0)){
      println("These two predicates are the same")
      nounMeanSim = 1.0
    }
    else {
      val nounLchSimilarity = wn.lchSimilarity(string1AsNoun, string2AsNoun)
      println("nounLchSimilarity = "+nounLchSimilarity)
      //println((nounLchSimilarity * 100).round / 100.toDouble)
      val nounWupSimilarity = wn.wupSimilarity(string1AsNoun, string2AsNoun)
      println("nounWupSimilarity = "+nounWupSimilarity)
      val nounResSimilarity = wn.resSimilarity(string1AsNoun, string2AsNoun)
      println("nounResSimilarity = "+nounResSimilarity)
      val nounJcnSimilarity = wn.jcnSimilarity(string1AsNoun, string2AsNoun)
      println("nounJcnSimilarity = "+nounJcnSimilarity)
      val nounLinSimilarity = wn.linSimilarity(string1AsNoun, string2AsNoun)
      println("nounLinSimilarity = "+nounLinSimilarity)
      val nounLeskSimilarity = wn.leskSimilarity(string1AsNoun, string2AsNoun)
      println("nounLeskSimilarity = "+nounLeskSimilarity)
      nounMeanSim = (nounPathSimilarity + nounLchSimilarity + nounWupSimilarity + nounResSimilarity + nounJcnSimilarity
        + nounLinSimilarity + nounLeskSimilarity) / 7
    }
    nounMeanSim
  }

  def arePredicatesEqual(string1: String, string2: String): Boolean = {
    var isEqual = false
    if (threshold < this.getMeanWordNetPredicateSimilarity(string1, string2)) {
      isEqual = true
    }
    isEqual
  }

  def setThreshold(newThreshold: Double): Unit = {
    threshold = newThreshold
  }

  def getThreshold: Double = threshold
}
