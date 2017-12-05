package net.sansa_stack.kgml.rdf

import breeze.linalg.max
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}

/**
  * Created by afshin on 26.10.17.
  */
class SimilarityHandler(initialThreshold: Double) {

  private var threshold = initialThreshold
  var maxLch = 3.6888794541139363
  var maxRes = 6.7959465490685735
  var maxJcn = 1.2876699500047589E7
  var maxLesk = 7


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

  def getMeanWordNetNounSimilarity(string1: String, string2: String): Double = {
    var nounMeanSim = 0.0
    try {
      val string1AsNoun = wn.synset(string1, POS.NOUN, 1)
      val string2AsNoun = wn.synset(string2, POS.NOUN, 1)
      val nounPathSimilarity = wn.pathSimilarity(string1AsNoun, string2AsNoun)
      val nounLchSimilarity = wn.lchSimilarity(string1AsNoun, string2AsNoun) / maxLch
      val nounWupSimilarity = wn.wupSimilarity(string1AsNoun, string2AsNoun)
      val nounResSimilarity = wn.resSimilarity(string1AsNoun, string2AsNoun) / maxRes
      val nounJcnSimilarity = wn.jcnSimilarity(string1AsNoun, string2AsNoun) / maxJcn
      val nounLinSimilarity = wn.linSimilarity(string1AsNoun, string2AsNoun)
      val nounLeskSimilarity = wn.leskSimilarity(string1AsNoun, string2AsNoun) / maxLesk
      nounMeanSim = (nounPathSimilarity + nounLchSimilarity + nounWupSimilarity + nounResSimilarity + nounJcnSimilarity
        + nounLinSimilarity + nounLeskSimilarity) / 7
//      println("nounPathSimilarity = "+nounPathSimilarity)
//      println("nounLchSimilarity = "+nounLchSimilarity)
//      println("nounWupSimilarity = " + nounWupSimilarity)
//      println("nounResSimilarity = " + nounResSimilarity)
//      println("nounJcnSimilarity = " + nounJcnSimilarity)
//      println("nounLinSimilarity = " + nounLinSimilarity)
//      println("nounLeskSimilarity = " + nounLeskSimilarity)

    }
    catch {
      case e: Exception => nounMeanSim = 0.0
    }


    nounMeanSim
  }

  def getMeanWordNetVerbSimilarity(string1: String, string2: String): Double = {
    var verbMeanSim = 0.0
    try{
      val string1AsVerb = wn.synset(string1, POS.VERB, 1)
      val string2AsVerb = wn.synset(string2, POS.VERB, 1)
      //7 similarity measures
      val verbPathSimilarity = wn.pathSimilarity(string1AsVerb, string2AsVerb)
      val verbLchSimilarity = wn.lchSimilarity(string1AsVerb, string2AsVerb) / maxLch
      val verbWupSimilarity = wn.wupSimilarity(string1AsVerb, string2AsVerb)
      val verbResSimilarity = wn.resSimilarity(string1AsVerb, string2AsVerb) / maxRes
      val verbJcnSimilarity = wn.jcnSimilarity(string1AsVerb, string2AsVerb) / maxJcn
      val verbLinSimilarity = wn.linSimilarity(string1AsVerb, string2AsVerb)
      val verbLeskSimilarity = wn.leskSimilarity(string1AsVerb, string2AsVerb) / maxLesk

      verbMeanSim = (verbPathSimilarity + verbLchSimilarity + verbWupSimilarity + verbResSimilarity + verbJcnSimilarity
        + verbLinSimilarity + verbLeskSimilarity) / 7

    }catch {
      case e: Exception => verbMeanSim = 0.0
    }
    verbMeanSim
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
