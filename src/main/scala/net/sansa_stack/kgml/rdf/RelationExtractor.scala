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

    //  var parentNode11 = df1.
    //  join(matchedSubject, df1("object1") === matchedSubject("subject3")).select("subject1", "predicate1", "object1")
    // parentNode11 = parentNode11.join(matchedSubject, parentNode11("subject1") =!= matchedSubject("subject3"))
    //  .select("subject1", "predicate1", "object1")

    matchedSubject.toDF("subject3")
    // extend by object
    val parentNode10 = df1.
      join(matchedSubject, df1("object1") === matchedSubject("subject3")).select("subject3")

    //extend one more step
    val parentNode11 = df1.
      join(parentNode10, df1("object1") === parentNode10("subject3") || df1("subject1") === parentNode10("subject3"))
      .select("subject1", "predicate1", "object1")

    // prune
    // parentNode11 = parentNode11.join(matchedSubject, parentNode11("subject1") =!= matchedSubject("subject3"))
    //  .select("subject1", "predicate1", "object1")

    //  var parentNode12 = df1.
    //    join(matchedSubject, df1("Subject1") === matchedSubject("subject3")).select("subject1", "predicate1", "object1")
    //parentNode12 = parentNode12.join(matchedSubject, parentNode12("object1") =!= matchedSubject("subject3"))
    // .select("subject1", "predicate1", "object1")

    // extend by subject
    var parentNode120 = df1.
      join(matchedSubject, df1("Subject1") === matchedSubject("subject3")).select("object1").toDF("subject3")

    //extend one more step
    var parentNode12 = df1.
      join(parentNode120, df1("object1") === parentNode120("subject3") || df1("subject1") === parentNode120("subject3"))
      .select("subject1", "predicate1", "object1")

    val parentNode1 = parentNode11.union(parentNode12).dropDuplicates("subject1", "predicate1", "object1")

    if (printReport) {
      println("Extracting more nodes to match...")
      parentNode1.show(30, 80)
    }
    parentNode1.persist()
  }

  //replace occurrence subject or objects in the first db that are matched by their matched counterparts from second df
  def replaceMatched(parentNodes1: DataFrame, subjectsMatch: DataFrame): DataFrame = {

    if (printReport) {
      println("extented nodes of first db before merging ontologies...")
      parentNodes1.show(30, 80)
    }
    val replaceMatched1 = parentNodes1.join(subjectsMatch, parentNodes1("subject1") === subjectsMatch("subject3"), "left")
      .select("subject4", "predicate1", "object1").toDF("subject1", "predicate1", "object1").where(col("subject1").isNotNull)

    val replaceMatched2 = replaceMatched1.join(subjectsMatch, replaceMatched1("object1") === subjectsMatch("subject3"), "left")
      .select("subject1", "predicate1", "subject4").toDF("subject1", "predicate1", "object1").where(col("object1").isNotNull)

    val replaceMatched3 = parentNodes1.join(subjectsMatch, parentNodes1("object1") === subjectsMatch("subject3"), "left")
      .select("subject1", "predicate1", "subject4").toDF("subject1", "predicate1", "object1").where(col("object1").isNotNull)

    val replaceMatched4 = replaceMatched3.join(subjectsMatch, replaceMatched3("subject1") === subjectsMatch("subject3"), "left")
      .select("subject4", "predicate1", "object1").toDF("subject1", "predicate1", "object1").where(col("subject1").isNotNull)

     var sum = replaceMatched1.union(replaceMatched2).union(replaceMatched3).union(replaceMatched4)
   // sum.show(40,60)

   val  rest =  parentNodes1.join(subjectsMatch,parentNodes1("object1") === subjectsMatch("subject3")  ||
     parentNodes1("subject1") === subjectsMatch("subject3"),"leftanti").select("subject1", "predicate1", "object1")

    val notConverted1 = sum.join(subjectsMatch, sum("subject1") === subjectsMatch("subject3")
     ).select("subject1", "predicate1", "object1")

    val notConverted2 = sum.join(subjectsMatch, sum("object1") === subjectsMatch("subject3")
    ).select("subject1", "predicate1", "object1")

    sum = sum.except(notConverted1).except(notConverted2). union(rest)

    // rest.show(40,60)
    //sum.show(40,60)

    if (printReport) {
      println("extending nodes of first db after merging ontologies...")
      sum.show(30, 80)
    }
    sum
  }

  //producing sameAs links
  def produceTriples(childSubjectsMatch: DataFrame): DataFrame = {

    var newTriples = childSubjectsMatch.withColumn("predicate", lit("<http://www.w3.org/2002/07/owl#sameAs>")).
      toDF("subject1", "object1", "predicate1")
    newTriples
  }
}
