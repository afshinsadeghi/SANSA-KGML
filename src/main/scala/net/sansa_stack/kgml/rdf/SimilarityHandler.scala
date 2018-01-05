package net.sansa_stack.kgml.rdf

import breeze.linalg.max
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}

/**
  * Created by afshin on 26.10.17.
  */
class SimilarityHandler(initialThreshold: Double) extends Serializable{

  private var threshold = initialThreshold
  var maxLch = 3.6888794541139363
  //var maxRes = 6.7959465490685735
  var maxRes = 8.993171126404793
  var maxJcn = 1.2876699500047589E7
  //var maxLesk = 7
  var maxLesk = 10


  val wn =  new WordNet()


  def checkLowerCaseStringEqulity(string1: String, string2: String): Boolean = {

    if(string1.length == 0 || string2.length == 0)return false
    string1.toLowerCase == string2.toLowerCase
  }

  def getPredicateSimilarity(string1: String, string2: String): Double = {
    var similarity = 0.0
    if (this.checkLowerCaseStringEqulity(string1, string2)) { // This is for phrases that has exactly same sequence of words

      similarity = 1.0
    } else {

      similarity = getMeanWordNetVerbOrNounSimilarity(string1, string2)
    }

    similarity
  }

  def getLiteralSimilarity(string1: String, string2: String): Double = {

    val string1l = this.removeSpecialChars(string1) // literals in difference KGs may be in double quotes etc.
    val string2l = this.removeSpecialChars(string2)

   val similarity = this.getPredicateSimilarity(string1l, string2l)
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

    (nounMeanSim * 100).round / 100.toDouble
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
    (verbMeanSim * 100).round / 100.toDouble
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

  def removeSpecialChars(string1: String): String = {
    val string2 = string1.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim
  string2
  }

  def jaccardSimilarity(intersectionCount : Double, num1 : Double, num2 : Double) : Double = {
    val union = num1 + num2 - intersectionCount
    intersectionCount / union
  }

  def splitCamelCase(s: String): Array[String] = {
    return s.replaceAll(
      String.format("%s|%s|%s",
        "(?<=[A-Z])(?=[A-Z][a-z])",
        "(?<=[^A-Z])(?=[A-Z])",
        "(?<=[A-Za-z])(?=[^A-Za-z])"
      ),
      " "
    ).replaceAll("  ", " ").replaceAll("  ", " ").split(" ")
  }


  def jaccardPredicateSimilarityWithWordNet( string1 : String, string2 : String ) : Double = {

    if(this.checkLowerCaseStringEqulity(string1, string2)) return 1.0

    var array1 = this.splitCamelCase(string1)
    var array2 = this.splitCamelCase(string2)
    val maxNumOfWordsInStringToTraverse = 5 //is added to make similarity check faster
    val maxTraverse1 = Math.min(maxNumOfWordsInStringToTraverse, array1.length )
    val maxTraverse2 = Math.min(maxNumOfWordsInStringToTraverse, array2.length )
    array1 = array1.take(maxTraverse1)
    array2 = array2.take(maxTraverse2)
    var intersectionCount = 0.0
    var localSim = 0.0
    for (x <- array1 ; y <- array2){
      localSim = this.getPredicateSimilarity(x, y)
      if (threshold <= localSim ) { intersectionCount =  intersectionCount + localSim}
    }
    //println(array1.toList, array2.toList)
    this.jaccardSimilarity(intersectionCount, array1.length, array2.length)
  }

  def jaccardLiteralSimilarityWithWordNet( string1 : String, string2 : String ) : Double = {

    if(this.checkLowerCaseStringEqulity(string1, string2)) return 1.0

    val string1l = this.removeSpecialChars(string1) // literals in difference KGs may be in double quotes etc.
    val string2l = this.removeSpecialChars(string2)
    if(string1l.length == 0 || string2l.length == 0) return 0.0

    this.jaccardPredicateSimilarityWithWordNet(string1l, string2l)
  }

}
