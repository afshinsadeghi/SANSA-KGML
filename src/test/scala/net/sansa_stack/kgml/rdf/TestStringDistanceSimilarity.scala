package net.sansa_stack.kgml.rdf

object TestStringDistanceSimilarity {


  def main(args: Array[String]): Unit = {

    var similarityThreshold = 0.6
    val similarityHandler = new SimilarityHandler(similarityThreshold)


    var longPredicate1 = "isCenterOf"
    var longPredicate2 = "isWorkingWith"

    var sim = similarityHandler.stringDistanceSimilarity(longPredicate1, longPredicate2)
    println("similarity of " + longPredicate1 + " and " + longPredicate2 + " is " + sim + "\n")

    longPredicate1 = "composer"
    longPredicate2 = "musicComposer"

    sim = similarityHandler.stringDistanceSimilarity(longPredicate1, longPredicate2)
    println("similarity of " + longPredicate1 + " and " + longPredicate2 + " is " + sim + "\n")

    longPredicate1 = "Afshin"
    longPredicate2 = "afshn"

    sim = similarityHandler.stringDistanceSimilarity(longPredicate1, longPredicate2)
    println("similarity of " + longPredicate1 + " and " + longPredicate2 + " is " + sim + "\n")

    longPredicate1 = "France"
    longPredicate2 = "French"

    sim = similarityHandler.stringDistanceSimilarity(longPredicate1, longPredicate2)
    println("similarity of " + longPredicate1 + " and " + longPredicate2 + " is " + sim + "\n")

  }


}
