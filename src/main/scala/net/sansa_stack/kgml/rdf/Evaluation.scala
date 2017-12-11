package net.sansa_stack.kgml.rdf
/*
* Created by Shimaa
*
* */

class Evaluation {
  def compressionRatio(totalNumber: Double, numberAfterGetSimilarity: Double): Double = {
    //println(totalNumber+" "+numberAfterGetSimilarity)

    val comR = (totalNumber - numberAfterGetSimilarity) / totalNumber*100

    ((100-comR) * 100).round / 100.toDouble
  }

}
