package net.sansa_stack.kgml.rdf

import breeze.linalg.max
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}
import org.apache.spark.sql.functions.udf

import scala.math._

/**
  * Created by afshin on 26.10.17.
  */
class SimilarityHandler(initialThreshold: Double) extends Serializable {

  private var stringSimThreshold = initialThreshold
  private var wordNetThreshold = initialThreshold - 0.20

  var maxLch = 3.6888794541139363
  //var maxRes = 6.7959465490685735
  var maxRes = 8.993171126404793
  var maxJcn = 1.2876699500047589E7
  //var maxLesk = 7
  var maxLesk = 19
  var maxHS = 200


  val stemmer = new Stemmer()
  val wn = new WordNet()


  def checkLowerCaseStringEquality(string1: String, string2: String): Boolean = {

    if (string1.length == 0 || string2.length == 0) return false
    string1.toLowerCase == string2.toLowerCase
  }

  def getPredicateSimilarity(string1: String, string2: String): Double = {
    var similarity = 0.0
    if (this.checkLowerCaseStringEquality(string1, string2)) { // This is for phrases that has exactly same sequence of words
      //println(string1 + " "+  string2 + "lower case equal")
      similarity = 1.0
    } else if (string1.length < 4 && string2.length < 4) {
      similarity = 0.0
    } else if (string1.length < 3 || string2.length < 3) {
      similarity = 0.0
    } else {

      similarity = getMeanWordNetVerbOrNounSimilarity(this.getRoot(string1), this.getRoot(string2))
     // similarity = getMeanWordNetVerbOrNounSimilarity(string1, string2)
      //var similarity = 0.0
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
      val nounLchSimilarity = wn.lchSimilarity(string1AsNoun, string2AsNoun) / maxLch    //LeacockChodorow  LC
      val nounWupSimilarity = wn.wupSimilarity(string1AsNoun, string2AsNoun)            //WuPalmer
      val nounResSimilarity = wn.resSimilarity(string1AsNoun, string2AsNoun) / maxRes   // Resnik R
      val nounJcnSimilarity = wn.jcnSimilarity(string1AsNoun, string2AsNoun) / maxJcn   // Jiang-Conrath JC
      val nounLinSimilarity = wn.linSimilarity(string1AsNoun, string2AsNoun)            // Lin L
      val nounLeskSimilarity = wn.leskSimilarity(string1AsNoun, string2AsNoun) / maxLesk  //Lesk
      val nounHSSimilarity = wn.hsSimlarity(string1AsNoun, string2AsNoun) / maxHS
      nounMeanSim = (nounPathSimilarity + nounLchSimilarity + nounWupSimilarity + nounResSimilarity + nounJcnSimilarity
        + nounLinSimilarity + nounLeskSimilarity + nounHSSimilarity) / 8
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
    try {
      val string1AsVerb = wn.synset(string1, POS.VERB, 1)
      val string2AsVerb = wn.synset(string2, POS.VERB, 1)
      //8 similarity measures
      val verbPathSimilarity = wn.pathSimilarity(string1AsVerb, string2AsVerb)
      val verbLchSimilarity = wn.lchSimilarity(string1AsVerb, string2AsVerb) / maxLch
      val verbWupSimilarity = wn.wupSimilarity(string1AsVerb, string2AsVerb)
      val verbResSimilarity = wn.resSimilarity(string1AsVerb, string2AsVerb) / maxRes
      val verbJcnSimilarity = wn.jcnSimilarity(string1AsVerb, string2AsVerb) / maxJcn
      val verbLinSimilarity = wn.linSimilarity(string1AsVerb, string2AsVerb)
      val verbLeskSimilarity = wn.leskSimilarity(string1AsVerb, string2AsVerb) / maxLesk
      val verbHSSimilarity = wn.hsSimlarity(string1AsVerb, string2AsVerb) / maxHS

      verbMeanSim = (verbPathSimilarity + verbLchSimilarity + verbWupSimilarity + verbResSimilarity + verbJcnSimilarity
        + verbLinSimilarity + verbLeskSimilarity + verbHSSimilarity) / 8

    } catch {
      case e: Exception => verbMeanSim = 0.0
    }
    (verbMeanSim * 100).round / 100.toDouble
  }

  def arePredicatesEqual(string1: String, string2: String): Boolean = {
    var isEqual = false
    if (wordNetThreshold < this.jaccardPredicateSimilarityWithWordNet(string1, string2)) {
      //println(wordNetThreshold)
      //println(this.jaccardPredicateSimilarityWithWordNet(string1, string2))
      isEqual = true
    }
    isEqual
  }


  def areLiteralsEqual(string1: String, string2: String): Boolean = {
    var isEqual = false
    if (stringSimThreshold < this.jaccardLiteralSimilarityWithLevenshtein(string1, string2)) {
      isEqual = true
    }
    isEqual
  }

  def setThreshold(newThreshold: Double): Unit = {
    stringSimThreshold = newThreshold
    wordNetThreshold = newThreshold - 0.2
  }

  def setWordNetThreshold(newThreshold: Double): Unit = {
    stringSimThreshold = newThreshold + 0.2
    wordNetThreshold = newThreshold
  }

  def getThreshold: Double = stringSimThreshold
  def getWordNetThreshold: Double = wordNetThreshold

  def removeSpecialChars(string1: String): String = {
    val string2 = string1.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim
    string2
  }

  def jaccardSimilarity(intersectionCount: Double, num1: Double, num2: Double): Double = {
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

  def getRoot(s: String): String = {
    stemmer.apply(s)
  }

  def jaccardPredicateSimilarityWithWordNet(string1: String, string2: String): Double = {

    if (this.checkLowerCaseStringEquality(string1, string2)) return 1.0

    val string1l = this.removeSpecialChars(string1) // literals in difference KGs may be in double quotes etc.
    val string2l = this.removeSpecialChars(string2)
    var array1 = this.splitCamelCase(string1l)
    var array2 = this.splitCamelCase(string2l)
    val maxNumOfWordsInStringToTraverse = 5 //the text is cut to make the similarity check faster
    val maxTraverse1 = Math.min(maxNumOfWordsInStringToTraverse, array1.length)
    val maxTraverse2 = Math.min(maxNumOfWordsInStringToTraverse, array2.length)

    if (maxTraverse1 > 1 || maxTraverse2 > 1) {

      val divider = Math.min(maxTraverse1, maxTraverse2)
      array1 = array1.take(maxTraverse1)
      array2 = array2.take(maxTraverse2)
      var intersectionCount = 0.0
      var localSim = 0.0
      for (x <- array1; y <- array2) {
        localSim = this.getPredicateSimilarity(x, y)
        if (wordNetThreshold <= localSim) {
          intersectionCount = intersectionCount + 1
        }
      }
      //println(array1.toList, array2.toList)
      this.jaccardSimilarity(intersectionCount, array1.length, array2.length)
    } else {
      this.getPredicateSimilarity(string1, string2)
    }
  }

  def jaccardLiteralSimilarityWithWordNet(string1: String, string2: String): Double = {

    if (this.checkLowerCaseStringEquality(string1, string2)) return 1.0

    val string1l = this.removeSpecialChars(string1) // literals in difference KGs may be in double quotes etc.
    val string2l = this.removeSpecialChars(string2) //for WordNet numbers also are meaningless
    if (string1l.length == 0 || string2l.length == 0) return 0.0

    var array1 = string1l.split(" ")
    var array2 = string2l.split(" ")
    val maxNumOfWordsInStringToTraverse = 5 //the text is cut to make the similarity check faster
    val maxTraverse1 = Math.min(maxNumOfWordsInStringToTraverse, array1.length)
    val maxTraverse2 = Math.min(maxNumOfWordsInStringToTraverse, array2.length)

    if (maxTraverse1 > 1 && maxTraverse2 > 1) {

      val divider = Math.min(maxTraverse1, maxTraverse2)
      array1 = array1.take(maxTraverse1)
      array2 = array2.take(maxTraverse2)
      var intersectionCount = 0.0
      var localSim = 0.0
      for (x <- array1; y <- array2) {
        localSim = this.getPredicateSimilarity(x, y)
        if (wordNetThreshold <= localSim) {
          intersectionCount = intersectionCount + 1
        }
      }
      //println(array1.toList, array2.toList)
      this.jaccardSimilarity(intersectionCount, array1.length, array2.length)
    } else {
      this.getPredicateSimilarity(string1, string2)
    }
  }

  /*

    string simlarity based on common string length
   */
  def stringDistanceSimilarity(s1: String, s2: String): Double = {
   val sim =  2 * (min(s1.length, s2.length) - levenshteinDistance(s1, s2)) / (s1.length + s2.length)
    if (sim < 0) 0 else sim
  }


  /*
    levenshtein string similarity measure
   */
  def levenshteinDistance(s1: String, s2: String): Double = {
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)

    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }
    for (j <- 1 to s2.length; i <- 1 to s1.length)
      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
    dist(s2.length)(s1.length)
  }

  def jaccardLiteralSimilarityWithLevenshtein(string1: String, string2: String): Double = {

    if (this.checkLowerCaseStringEquality(string1, string2)) return 1.0

    val string1l = this.removeSpecialChars(string1) // literals in difference KGs may be in double quotes etc.
    val string2l = this.removeSpecialChars(string2)
    if (string1l.length == 0 || string2l.length == 0) return 0.0

    var array1 = string1l.split(" ")
    var array2 = string2l.split(" ")
    val maxNumOfWordsInStringToTraverse = 5 //the text is cut to make the similarity check faster
    val maxTraverse1 = Math.min(maxNumOfWordsInStringToTraverse, array1.length)
    val maxTraverse2 = Math.min(maxNumOfWordsInStringToTraverse, array2.length)

    if (maxTraverse1 > 1 && maxTraverse2 > 1) {

      val divider = Math.min(maxTraverse1, maxTraverse2)
      array1 = array1.take(maxTraverse1)
      array2 = array2.take(maxTraverse2)
      var intersectionCount = 0.0
      var localSim = 0.0
      for (x <- array1; y <- array2) {
        localSim = this.stringDistanceSimilarity(x, y)
        if (stringSimThreshold <= localSim) {
          intersectionCount = intersectionCount + 1
        }
      }
      //println(array1.toList, array2.toList)
      this.jaccardSimilarity(intersectionCount, array1.length, array2.length)
    } else {
      this.stringDistanceSimilarity(string1, string2)
    }
  }

  def getSimilarity(s1: String, s2: String): Double = {
    var sim = 0.0
    if (s1.startsWith("<")) { //break down # part
      var s11 = s1.split("<")(1)
      var s12 = s2.split("<")(1)
      if(!s11.isEmpty && !s12.isEmpty){
         sim = jaccardPredicateSimilarityWithWordNet(s11, s12)
        if (sim > wordNetThreshold) sim = stringSimThreshold
      }else sim = 0.0
      //In WordNet mean similarity measure greater values than 0.5 are similar but in distance similarity, higher than 0.7 is similar
      //WordNet alone must not be accepted as similar, so the aggregate threshold should it more than threshold(0.7).
      sim
    }
    else jaccardLiteralSimilarityWithLevenshtein(s1, s2)
  }

  val getSimilarityUDF = udf((S: String, S2: String) => {
    jaccardLiteralSimilarityWithLevenshtein(S,S2)
    //getSimilarity(S, S2)
  })

  val getWordNetSimilarityUDF = udf((S: String, S2: String) => {
    jaccardLiteralSimilarityWithWordNet(S,S2)
  })

  val getPredicateWordNetSimilarityUDF = udf((S: String, S2: String) => {
    jaccardPredicateSimilarityWithWordNet(S,S2)
  })

  val getExactMatchUDF = udf((S: String, S2: String) => {
    if (checkLowerCaseStringEquality(S,S2)) 1
    else 0
  })



}
