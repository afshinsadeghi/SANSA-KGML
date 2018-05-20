package net.sansa_stack.kgml.rdf

import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.{Synset, WordNet}

object WordNetExperiment {
  val wn = new WordNet()

  def main(args: Array[String]) = {
    var words = Array("cord", "smile",
      "rooster", "voyage",
      "noon", "string",
      "fruit", "furnace",
      "autograph", "shore",
      "automobile", "wizard",
      "mound", "stove",
      "grin", "implement",
      "asylum", "fruit",
      "asylum", "monk",
      "graveyard", "madhouse",
      "glass", "magician",
      "boy", "rooster",
      "cushion", "jewel",
      "monk", "slave",
      "asylum", "cemetery",
      "coast", "forest",
      "grin", "lad",
      "shore", "woodland",
      "monk", "oracle",
      "boy", "sage",
      "automobile", "cushion",
      "mound", "shore",
      "lad", "wizard",
      "forest", "graveyard",
      "food", "rooster",
      "cemetery", "woodland",
      "shore", "voyage",
      "bird", "woodland",
      "coast", "hill",
      "furnace", "implement",
      "crane", "rooster",
      "hill", "woodland",
      "car", "journey",
      "cemetery", "mound",
      "glass", "jewel",
      "magician", "oracle",
      "crane", "implement",
      "brother", "lad",
      "sage", "wizard",
      "oracle", "sage",
      "bird", "crane",
      "bird", "cock",
      "food", "fruit",
      "brother", "monk",
      "asylum", "madhouse",
      "furnace", "stove",
      "magician", "wizard",
      "hill", "mound",
      "cord", "string",
      "glass", "tumbler",
      "grin", "smile",
      "serf", "slave",
      "journey", "voyage",
      "autograph", "signature",
      "coast", "shore",
      "forest", "woodland",
      "implement", "tool",
      "cock", "rooster",
      "boy", "lad",
      "cushion", "pillow",
      "cemetery", "graveyard",
      "automobile", "car",
      "midday", "noon",
      "gem", "jewel")

    var x = 0
    var max = words.length
    for (x <- 0 until max by 2) {
      printSimValues(words(x), words(x + 1))
    }
  }

  def printSimValues(string1: String, string2: String): Unit = {


    val string1AsNoun = wn.synset(string1, POS.NOUN, 1)
    val string2AsNoun = wn.synset(string2, POS.NOUN, 1)
    //val nounPathSimilarity = wn.pathSimilarity(string1AsNoun, string2AsNoun)
    val nounLchSimilarity = wn.lchSimilarity(string1AsNoun, string2AsNoun) //LeacockChodorow  LC
    val nounWupSimilarity = wn.wupSimilarity(string1AsNoun, string2AsNoun) //WuPalmer
    val nounResSimilarity = wn.resSimilarity(string1AsNoun, string2AsNoun) // Resnik R
    val nounJcnSimilarity = wn.jcnSimilarity(string1AsNoun, string2AsNoun) // Jiang-Conrath JC
    val nounLinSimilarity = wn.linSimilarity(string1AsNoun, string2AsNoun) // Lin L
    val nounLeskSimilarity = wn.leskSimilarity(string1AsNoun, string2AsNoun) //Lesk
    val nounHSSimilarity = wn.hsSimlarity(string1AsNoun, string2AsNoun)
    println(string1 + ", " + string2 + ", " + nounHSSimilarity + ", " + nounJcnSimilarity + ", " +
      nounLchSimilarity + ", " + nounLinSimilarity + ", " +
      nounResSimilarity + ", " + nounWupSimilarity + ", " + nounLeskSimilarity)
  }

}
