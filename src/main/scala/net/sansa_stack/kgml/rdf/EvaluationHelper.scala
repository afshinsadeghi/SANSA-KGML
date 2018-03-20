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

}
