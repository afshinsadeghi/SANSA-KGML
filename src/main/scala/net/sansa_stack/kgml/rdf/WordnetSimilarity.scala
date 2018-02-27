package net.sansa_stack.kgml.rdf

import net.sansa_stack.kgml.rdf.wordnet.WordNet

class WordnetSimilarity extends Serializable {
  var maxLch = 3.6888794541139363
  //var maxRes = 6.7959465490685735
  var maxRes = 8.993171126404793
  var maxJcn = 1.2876699500047589E7
  //var maxLesk = 7
  var maxLesk = 10
  //val wn = WordNet()

  def getPredicateSimilarity(string1: String, string2: String): Double = {
    var similarity = 0.0
    if (string1.toLowerCase == string2.toLowerCase) { // This is for phrases that has exactly same sequence of words

      similarity = 1.0
    } else {

      //similarity = getMeanWordNetVerbOrNounSimilarity(string1, string2)
      var similarity = 0.0
    }

    similarity
  }
}
