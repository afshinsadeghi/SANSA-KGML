package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class RelationExtractor extends EvaluationHelper {
  //get parents of a matched entity and pair it with parents of its equivalent in the second data set
  /**
    * going one step in the neighborhood and getting pair of parents or child of matched entities to compare
    *
    * @param df1
    * @return
    */
  def getParentEntities(df1: DataFrame, matchedSubject: DataFrame): DataFrame = {
    //get parent nodes, removing from them those subjects the matched entities from df1 and df2 subjects
    //  val matchedSubject = leafSubjectsMatch.toDF("Subject3", "Subject4", "normStrSim").drop("normStrSim")

    var parentNode11 = df1.
      join(matchedSubject, df1("object1") === matchedSubject("subject3")).select("subject1", "predicate1", "object1")
    //.select("subject1","object1")
    parentNode11 = parentNode11.join(matchedSubject, parentNode11("subject1") =!= matchedSubject("subject3"))
      .select("subject1", "predicate1", "object1")

    var parentNode12 = df1.
      join(matchedSubject, df1("Subject1") === matchedSubject("subject3")).select("subject1", "predicate1", "object1")
    //.select("subject1","object1")
    parentNode12 = parentNode12.join(matchedSubject, parentNode12("object1") =!= matchedSubject("subject3"))
      .select("subject1", "predicate1", "object1")

    val parentNode1 = parentNode11.union(parentNode12)

    if (printReport) {
      println("Extracting more nodes to match...")
      parentNode1.show(20, 80)
    }
    parentNode1.persist()
  }

  //producing sameAs links
  def produceTriples(childSubjectsMatch: DataFrame): DataFrame = {

    var newTriples = childSubjectsMatch.withColumn("predicate", lit("<http://www.w3.org/2002/07/owl#sameAs>")).
      toDF("subject1", "object1", "predicate1")
    newTriples
  }
}
