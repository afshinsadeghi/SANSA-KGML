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
    val wordNetSim = new SimilarityHandler(0.7)
    wordNetSim.arePredicatesEqual(ending1, ending2)
  })


  /*
  * get literal value in objects
   */
  val getLiteralValue = udf((S: String) => {
    //println("input:" + S)
    if (S == null) null
    else if (S.length > 0 && S.startsWith("<")) {
      //println("non literal:" + S)
      null
    } else {
      //println("literal:" + S)
      if (S.length == 0) null
      else if( S.startsWith("\"")){
        val str = S.split("\"")
        if (str == null || str.isEmpty) S
        else
          str(1)
      }else{
        S  //some file does not have literals in quotation
      }
    }
  })


  def getURIEnding(str: String): String = {
    var ending1 = str.split("<")(1).split(">")(0).split("/").last
    if (ending1.endsWith(" .")) ending1 = ending1.drop(2)
    //println(ending1)
    ending1
  }

  def getMatchedPredicatesWithSplit(df1: DataFrame, df2: DataFrame): Unit = {

    //1. First filter all predicates in one column dataframes A and B, I expect all fit into memory
    //2. make a cartesian comparison of all them.

    val dF1 = df1.select(df1("predicate1")).distinct.coalesce(5).persist()
    //  .withColumn("predicate_ending", getLastPartOfURI(col("object1")))

    val dF2 = dF1.crossJoin(df2.select(df2("predicate2")).distinct).coalesce(5).persist()
    //    .withColumn("predicate_ending", getLastPartOfURI(col("object2")))

    val splitDF = dF2.randomSplit(Array(1, 1, 1, 1))
    val (dfs1, dfs2, dfs3, dfs4) = (splitDF(0), splitDF(1), splitDF(2), splitDF(3))


    val dfp1 = dfs1.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2"))).coalesce(5).persist()
    val dfp2 = dfs2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2"))).coalesce(5).persist()
    val dfp3 = dfs3.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2"))).coalesce(5).persist()
    val dfp4 = dfs4.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2"))).coalesce(5).persist()

    // val dF3 = dF2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2")))

    //dF3.createOrReplaceTempView("triple")

    dfp1.createOrReplaceTempView("triple1")
    dfp2.createOrReplaceTempView("triple2")
    dfp3.createOrReplaceTempView("triple3")
    dfp4.createOrReplaceTempView("triple4")

    //val sqlText2 = "SELECT same_predicate, COUNT(*) FROM triple group by same_predicate ORDER BY COUNT(*) DESC"
    //val dPredicateStats = sparkSession.sql(sqlText2)
    //   dPredicateStats.show(15, 80)

    val sqlText1 = "SELECT same_predicate, COUNT(*) FROM triple1 group by same_predicate ORDER BY COUNT(*) DESC"
    val dPredicateStats1 = sparkSession.sql(sqlText1)

    val sqlText2 = "SELECT same_predicate, COUNT(*) FROM triple2 group by same_predicate ORDER BY COUNT(*) DESC"
    val dPredicateStats2 = sparkSession.sql(sqlText2)

    val sqlText3 = "SELECT same_predicate, COUNT(*) FROM triple3 group by same_predicate ORDER BY COUNT(*) DESC"
    val dPredicateStats3 = sparkSession.sql(sqlText3)

    val sqlText4 = "SELECT same_predicate, COUNT(*) FROM triple4 group by same_predicate ORDER BY COUNT(*) DESC"
    val dPredicateStats4 = sparkSession.sql(sqlText4)

    val predicates = dPredicateStats1.union(dPredicateStats2).union(dPredicateStats3).union(dPredicateStats4).coalesce(5)
    println(predicates.collect().take(200))


  }

  /**
    * Returns a two column dataFrame of matched predicates
    *
    * @param df1
    * @param df2
    * @return
    */
  def getMatchedPredicates(df1: DataFrame, df2: DataFrame): DataFrame = {

    //1. First filter all predicates in one column dataframes A and B, I expect all fit into memory
    //2. make a cartesian comparison of all them.
    //df1.show(20, 80)
    //df2.show(20,80)
    val dF1 = df1.select(df1("predicate1")).distinct.coalesce(5).persist()
    //  .withColumn("predicate_ending", getLastPartOfURI(col("object1")))

    val dF2 = dF1.crossJoin(df2.select(df2("predicate2")).distinct).coalesce(5).persist()
    //    .withColumn("predicate_ending", getLastPartOfURI(col("object2")))

    // val dF3 = dF2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2")))

    //dF3.createOrReplaceTempView("triple")


    //val sqlText2 = "SELECT same_predicate, COUNT(*) FROM triple group by same_predicate ORDER BY COUNT(*) DESC"
    //val predicates = sparkSession.sql(sqlText2)
    //predicates.show(15, 80)

    //println(predicates.collect().take(20))


    val wordNetSim = new SimilarityHandler(0.4)
    val similarPairs = dF2.collect().map(x => (x.getString(0), x.getString(1),
      wordNetSim.arePredicatesEqual(getURIEnding(x.getString(0)),
        getURIEnding(x.getString(1)))))

    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val matched = rdd1.toDF("predicate1", "predicate2", "equal")

    // matched.show(40)

    matched.createOrReplaceTempView("triple1")
    val sqlText2 = "SELECT predicate1, predicate2 FROM triple1 where equal = true"
    val predicates = sparkSession.sql(sqlText2)
    predicates.show(50, 80)


    /*

    The result between drugdunmp dataset and dbpedia
+----------------------------------------------------------------------------+-------------------------------------------------+
|                                                                  predicate1|                                       predicate2|
+----------------------------------------------------------------------------+-------------------------------------------------+
|                           <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/molecularWeight>|    <http://dbpedia.org/property/molecularWeight>|
|                                      <http://www.w3.org/2002/07/owl#sameAs>|           <http://www.w3.org/2002/07/owl#sameAs>|
|   <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint>|       <http://dbpedia.org/property/meltingPoint>|
|         <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target>|             <http://dbpedia.org/property/target>|
|                                <http://www.w3.org/2000/01/rdf-schema#label>|     <http://www.w3.org/2000/01/rdf-schema#label>|
|           <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/name>|                 <http://xmlns.com/foaf/0.1/name>|
|           <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/name>|               <http://dbpedia.org/property/name>|
+----------------------------------------------------------------------------+-------------------------------------------------+




           The output for exact string equality on apple debeida:
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
    predicates
  }


  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  def BlockSubjectsByTypeAndLiteral(df1: DataFrame, df2: DataFrame, matchedPredicates: DataFrame): Unit = {

    val dF1 = df1.
      withColumn("Literal1", getLiteralValue(col("object1")))

    var dF2 = df2.
      withColumn("Literal2", getLiteralValue(col("object2")))

   // dF2 = dF2.where($"subject2".contains("http://dbpedia.org/resource/2C-B-BUTTERFLY>"))

    //dF1.show(60, 40)
    //dF2.show(60, 40)

    dF1.createOrReplaceTempView("triple")
    dF2.createOrReplaceTempView("triple2")
    val samePredicateAndObject = dF1.
      join(matchedPredicates, dF1("predicate1") <=> matchedPredicates("predicate1")).
      join(dF2, matchedPredicates("predicate2") <=> dF2("predicate2") && dF1("Literal1")<=> dF2("Literal2") && dF1("Literal1").isNotNull)

    samePredicateAndObject.show(70, 50)

    samePredicateAndObject.createOrReplaceTempView("sameTypes")
    println("Those subjects that are matched by matched predicate and matched literals")
    val sqlText2 = "SELECT subject1, subject2 FROM sameTypes "
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)

    println("the number of matched pair of subjects that are matched by matched predicate and matched literals")
    val sqlText3 = "SELECT count(*) FROM sameTypes "
    val typedTriples3 = sparkSession.sql(sqlText3)
    typedTriples3.show()
  }

  def getMatchedEntities(df1: DataFrame, df2: DataFrame, matchedPredicates: DataFrame): Unit = {


  }
}
