package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by afshin on 07.03.18.
  */
class Matching(sparkSession: SparkSession) {


  val wordNetPredicateMatch = udf((S: String, S2: String) => {

    var ending1 = S.split("<")(1).split(">")(0)
    var ending2 = S2.split("<")(1).split(">")(0)
    ending1.equals(ending2)
    val wordNetSim = new SimilarityHandler(0.7)
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

    val dF2 = dF1.crossJoin(df2.select(df2("predicate2")).distinct)
    //    .withColumn("predicate_ending", getLastPartOfURI(col("object2")))

    val dF3 = dF2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2")))
    // dF3.show()
    dF3.createOrReplaceTempView("triple")


    val sqlText2 = "SELECT same_predicate, COUNT(*) FROM triple group by same_predicate ORDER BY COUNT(*) DESC"
    val dPredicateStats = sparkSession.sql(sqlText2)
    dPredicateStats.show(15, 80)

/*
       The output for exact string equality :
   +--------------+--------+
   |same_predicate|count(1)|
   +--------------+--------+
   |         false|   27636|
   |          true|       3|
   +--------------+--------+
 */

/*


    val sqlText3 = "SELECT predicate1 FROM triple where same_predicate = true"
    val samePredicates = sparkSession.sql(sqlText3)
    samePredicates.show(15, 80)

    The output :
      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
               <http://www.w3.org/2002/07/owl#sameAs>
         <http://www.w3.org/2000/01/rdf-schema#label>

 */


}
}
