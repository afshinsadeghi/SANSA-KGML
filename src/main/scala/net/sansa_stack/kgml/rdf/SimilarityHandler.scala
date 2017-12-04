package net.sansa_stack.kgml.rdf

import breeze.linalg.max
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}

/**
  * Created by afshin on 26.10.17.
  */
class SimilarityHandler(initialThreshold: Double) {

  private var threshold = initialThreshold

  val wn = WordNet()

  def getPredicateSimilarity(string1: String, string2: String): Double = {
    var similarity = 0.0
    if (string1.toLowerCase == string2.toLowerCase) { // This is for phrases that has exactly same sequence of words

      similarity = 1.0
    } else {

      similarity = getMeanWordNetVerbOrNounSimilarity(string1, string2)
    }

    similarity
  }

  // predicates can be a verb or a noun
  def getMeanWordNetVerbOrNounSimilarity(string1: String, string2: String): Double = {

    val meanSim = max(this.getMeanWordNetNounSimilarity(string1, string2), this.getMeanWordNetVerbSimilarity(string1, string2))
    meanSim
  }

  def getMeanWordNetVerbSimilarity(string1: String, string2: String): Double = {
    var verbMeanSim = 0.0
    try{
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

       verbMeanSim = (verbPathSimilarity + verbLchSimilarity + verbWupSimilarity + verbResSimilarity + verbJcnSimilarity
        + verbLinSimilarity + verbLeskSimilarity) / 7

    }catch {
      case e: Exception => verbMeanSim = 0.0
    }
    verbMeanSim
  }

  //
  def getMeanWordNetNounSimilarity(string1: String, string2: String): Double = {
    var nounMeanSim = 0.0
    try {
      val string1AsNoun = wn.synset(string1, POS.NOUN, 1)
      val string2AsNoun = wn.synset(string2, POS.NOUN, 1)
      val nounPathSimilarity = wn.pathSimilarity(string1AsNoun, string2AsNoun)
      // println("nounPathSimilarity = "+nounPathSimilarity)
      val nounLchSimilarity = wn.lchSimilarity(string1AsNoun, string2AsNoun) / 3.689
      //println("nounLchSimilarity = "+nounLchSimilarity)
      //println((nounLchSimilarity * 100).round / 100.toDouble)
      val nounWupSimilarity = wn.wupSimilarity(string1AsNoun, string2AsNoun)
      //println("nounWupSimilarity = " + nounWupSimilarity)
      val nounResSimilarity = wn.resSimilarity(string1AsNoun, string2AsNoun) / 7.75
      //println("nounResSimilarity = " + nounResSimilarity)
      val nounJcnSimilarity = wn.jcnSimilarity(string1AsNoun, string2AsNoun) / 12876699.6
      //println("nounJcnSimilarity = " + nounJcnSimilarity)
      val nounLinSimilarity = wn.linSimilarity(string1AsNoun, string2AsNoun)
      //println("nounLinSimilarity = " + nounLinSimilarity)
      val nounLeskSimilarity = wn.leskSimilarity(string1AsNoun, string2AsNoun) / 8
      //println("nounLeskSimilarity = " + nounLeskSimilarity)
      nounMeanSim = (nounPathSimilarity + nounLchSimilarity + nounWupSimilarity + nounResSimilarity + nounJcnSimilarity
        + nounLinSimilarity + nounLeskSimilarity) / 7

    }
    catch {
      case e: Exception => nounMeanSim = 0.0
    }


    nounMeanSim
  }

  def arePredicatesEqual(string1: String, string2: String): Boolean = {
    var isEqual = false
    if (threshold < this.getPredicateSimilarity(string1, string2)) {
      isEqual = true
    }
    isEqual
  }

  def setThreshold(newThreshold: Double): Unit = {
    threshold = newThreshold
  }

  def getThreshold: Double = threshold
}
