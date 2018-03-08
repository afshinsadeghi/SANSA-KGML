package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by afshin on 07.03.18.
  */
class Matching(sparkSession: SparkSession) {


  /*
  *  get last part of a URI
  */
  val getLastPartOfURI = udf((S: String) => {
    if (S.startsWith("<")) {
      var temp = S.split("<")(1)
      temp = temp.split(">")(0)
      temp = temp.split("\\").last
    }
    else S
  })

  var wordNetSim = new  SimilarityHandler(0.7)

  val wordNetPredicateMatch = udf((S: String, S2: String) => {

    var ending1 = S.split("<")(1).split(">")(0)
    var ending2 = S2.split("<")(1).split(">")(0)
    wordNetSim.arePredicatesEqual(ending1, ending2)
  })

  val getLiteralValue = udf((S: String) => {
    if (S.startsWith("\"")) S.split("\"")(1) else S
  })

  def getMatchedPredicates(df1: DataFrame, df2: DataFrame): Unit = {

    //1. First filter all predicates in one column dataframes A and B, I expect all fit into memory
    //2. make a cartesian comparison of all them.

    val dF1 = df1.select(df1("predicate1")).distinct
    //  .withColumn("predicate_ending", getLastPartOfURI(col("object1")))

    val dF2 = dF1.join(df2.select(df2("predicate2")).distinct)
    //    .withColumn("predicate_ending", getLastPartOfURI(col("object2")))

     val dF3= dF2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2") ))

       dF3.show(80, 20)
    dF3.createOrReplaceTempView("triple")

/*
    val samePredicateAndObject = dF1.join(dF2, dF1("predicate1") <=> dF2("predicate2")
      && dF1("literal1") <=> dF2("literal2"))

    samePredicateAndObject.createOrReplaceTempView("sameTypes")
    println("ranking of subjects based on common type.(I used common predicate and objects which is more general than common type)")
    val sqlText2 = "SELECT  subject1, subject2, COUNT(*) FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)
*/
  }
}
