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
      try { //handling special case of Drugbank that puts casRegistryName in URIs, matching between literlas and uri
        var str = S.split("<")(1).split(">")(0).split("/").last
        if (str.endsWith(" .")) str = str.drop(2)
        str
        //println("non literal:" + S)
      } catch {
        case e: Exception =>  null
      }
    } else {
      //println("literal:" + S)
      if (S.length == 0) null
      else if (S.startsWith("\"")) {
        val str = S.split("\"")
        if (str == null || str.isEmpty) S
        else
          str(1)
      } else {
        S //some file does not have literals in quotation
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


    val wordNetSim = new SimilarityHandler(0.5)
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

  def clusterRankSubjects(SubjectsWithLiteral: DataFrame ): DataFrame ={

    SubjectsWithLiteral.createOrReplaceTempView("sameTypes")

    println("ranking of subjects based on their common type.(I used common predicate and objects which is more general than common type)")
    val sqlText2 = "SELECT  subject1, subject2, COUNT(*) as count FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
    val blockedSubjects2 = sparkSession.sql(sqlText2)
    blockedSubjects2.show(15, 80)
    /*
    Result for drugbank
+-----------------------------------------------------------------+--------------------------------------------------------------+--------+
|                                                         subject1|                                                      subject2|count(1)|
+-----------------------------------------------------------------+--------------------------------------------------------------+--------+
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|                            <http://dbpedia.org/resource/2C-B>|    1268|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01248>|                            <http://dbpedia.org/resource/2C-B>|    1146|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|                        <http://dbpedia.org/resource/3-sphere>|    1057|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00515>|                            <http://dbpedia.org/resource/2C-B>|    1034|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|             <http://dbpedia.org/resource/3,4-Diaminopyridine>|     980|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|<http://dbpedia.org/resource/2,5-Dimethoxy-4-bromoamphetamine>|     964|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01248>|                        <http://dbpedia.org/resource/3-sphere>|     957|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|                            <http://dbpedia.org/resource/2C-D>|     955|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00441>|                            <http://dbpedia.org/resource/2C-B>|     940|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|   <http://dbpedia.org/resource/3,4-Methylenedioxyamphetamine>|     933|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01248>|             <http://dbpedia.org/resource/3,4-Diaminopyridine>|     890|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00531>|                            <http://dbpedia.org/resource/2C-B>|     885|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01248>|<http://dbpedia.org/resource/2,5-Dimethoxy-4-bromoamphetamine>|     872|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01248>|                            <http://dbpedia.org/resource/2C-D>|     863|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00073>|                            <http://dbpedia.org/resource/2C-B>|     859|
+-----------------------------------------------------------------+--------------------------------------------------------------+--------+
only showing top 15 rows
     */
    blockedSubjects2
  }

  /**
    * block triples based on matched predicates
    * @param df1
    * @param df2
    * @param matchedPredicates
    */
  def BlockSubjectsByTypeAndLiteral(df1: DataFrame, df2: DataFrame, matchedPredicates: DataFrame): DataFrame = {

    val dF1 = df1.
      withColumn("Literal1", getLiteralValue(col("object1")))

    var dF2 = df2.
      withColumn("Literal2", getLiteralValue(col("object2")))

    // dF2 = dF2.where($"subject2".contains("http://dbpedia.org/resource/2C-B-BUTTERFLY>"))

    //dF1.show(60, 40)
    //dF2.show(60, 40)
    val predicatesPairs = matchedPredicates.toDF("Predicate3", "Predicate4")
    dF1.createOrReplaceTempView("triple")
    dF2.createOrReplaceTempView("triple2")
    val samePredicateSubjectObjects = dF1.
      join(predicatesPairs, dF1("predicate1") <=> predicatesPairs("Predicate3")).
      join(dF2, predicatesPairs("Predicate4") <=> dF2("predicate2") && dF2("Literal2").isNotNull && dF1("Literal1").isNotNull)

    samePredicateSubjectObjects.show(70, 50)

    samePredicateSubjectObjects.createOrReplaceTempView("sameTypes")
    println("the number of matched pair of subjects that are matched by matched predicate")
    val sqlText3 = "SELECT count(*) FROM sameTypes "
    val typedTriples3 = sparkSession.sql(sqlText3)
    typedTriples3.show()

    //select from  "Subject1","Predicate1","Object1","Literal1", "Predicate3","Predicate4","Subject2","Predicate2","Object2","Literal2"

    val typeSubjectWithLiteral = samePredicateSubjectObjects.select( "Subject1","Predicate1","Literal1", "Subject2","Predicate2","Literal2")
    typeSubjectWithLiteral
  }

  def getMatchedEntities( typeSubjectWithLiteral: DataFrame, matchedPredicates : DataFrame): DataFrame = {


    val clusteredSubjects = this.clusterRankSubjects(typeSubjectWithLiteral)
    val clusters = clusteredSubjects.toDF("Subject4", "Subject5", "count")

    val firstMatchingLevel = typeSubjectWithLiteral.join(clusters,
      typeSubjectWithLiteral("subject1") <=> clusters("Subject4") &&
        typeSubjectWithLiteral("subject2") <=> clusters("Subject5") &&
        clusters("count") < 5 && clusters("count") > 2 //many of them have count 1 and those that have a big number of comparison also are taking the memory
    )    // block typeSubjectWithLiteral here based on clusteredSubjects
// redistribute result of distribution such that gain cluster of equal size

    firstMatchingLevel.show(40,80)
    val wordNetSim = new SimilarityHandler(0.5)
    val similarPairs = firstMatchingLevel.collect().map(x => (x.getString(0), x.getString(2),
      wordNetSim.areLiteralsEqual(x.getString(1), x.getString(3))))

    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val matched = rdd1.toDF("Subject1", "Subject2", "equal")

    matched.show(40)

    matched.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2 FROM matched where equal = true"
    val subjects = sparkSession.sql(sqlText1)
    subjects.show(50, 80)

    println("the number of matched pair of subjects that are matched by matched predicate and matched literals")
    val sqlText3 = "SELECT count(*) FROM matched "
    val typedTriples3 = sparkSession.sql(sqlText3)
    typedTriples3.show()
    subjects
  }
}
