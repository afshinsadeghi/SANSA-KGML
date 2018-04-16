package net.sansa_stack.kgml.rdf

trait EvaluationHelper {


  var printReport = true

  def profile[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.00 + "s")
    result
  }

  def disablePrintReport(): Unit ={
    this.printReport = false
  }

  def enablePrintReport(): Unit ={
    this.printReport = true
  }

  // This is for evaluation experiment to compare exact match and wordnet based match
  // put both to false to use string distance matching
  var exactMatchEvaluation = false
  var wordNetMatchEvaluation = false

  var comparisonCounter = 0
}
