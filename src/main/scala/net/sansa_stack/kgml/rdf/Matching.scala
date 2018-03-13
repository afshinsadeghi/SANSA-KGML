package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

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
        case e: Exception => null
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
    //val dF1 = df1.select(df1("predicate1")).distinct.coalesce(5).persist()
    //  .withColumn("predicate_ending", getLastPartOfURI(col("object1")))

    //val dF2 = dF1.crossJoin(df2.select(df2("predicate2")).distinct).coalesce(5).persist()
    val dF2 = (df1.select(df1("predicate1")).distinct).crossJoin(df2.select(df2("predicate2")).distinct)
    println("number of partitions after cross join = " + dF2.rdd.partitions.size) //200 partition
    //Elapsed time: 90.543716752s
    //Elapsed time: 85.588292884s without coalesce(10)
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
    //println("matched predicates:")
    //matched.show(40)
    //Elapsed time: 92.068153666s
    //Elapsed time: 103.122292326s with using cache
    println("number of partitions for matched predicates = " + matched.rdd.partitions.size)

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

in Persons dataset:

+---------------------------------------------------------+---------------------------------------------------------+
|                                               predicate1|                                               predicate2|
+---------------------------------------------------------+---------------------------------------------------------+
|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|
|  <http://www.okkam.org/ontology_person1.owl#phone_numer>|  <http://www.okkam.org/ontology_person2.owl#phone_numer>|
|          <http://www.okkam.org/ontology_person1.owl#age>|          <http://www.okkam.org/ontology_person2.owl#age>|
|   <http://www.okkam.org/ontology_person1.owl#given_name>|   <http://www.okkam.org/ontology_person2.owl#given_name>|
|     <http://www.okkam.org/ontology_person1.owl#postcode>|     <http://www.okkam.org/ontology_person2.owl#postcode>|
|  <http://www.okkam.org/ontology_person1.owl#has_address>|  <http://www.okkam.org/ontology_person2.owl#has_address>|
|      <http://www.okkam.org/ontology_person1.owl#surname>|      <http://www.okkam.org/ontology_person2.owl#surname>|
|       <http://www.okkam.org/ontology_person1.owl#street>|       <http://www.okkam.org/ontology_person2.owl#street>|
|<http://www.okkam.org/ontology_person1.owl#date_of_birth>|<http://www.okkam.org/ontology_person2.owl#date_of_birth>|
| <http://www.okkam.org/ontology_person1.owl#house_number>| <http://www.okkam.org/ontology_person2.owl#house_number>|
|   <http://www.okkam.org/ontology_person1.owl#soc_sec_id>|   <http://www.okkam.org/ontology_person2.owl#soc_sec_id>|
+---------------------------------------------------------+---------------------------------------------------------+


           The output for exact string equality on apple DBpeida:
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

  def clusterRankSubjects(SubjectsWithLiteral: DataFrame): DataFrame = {

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

  def clusterRankCounts(rankedSubjectWithCommonPredicateCount: DataFrame): DataFrame = {
    rankedSubjectWithCommonPredicateCount.createOrReplaceTempView("groupedSubjects")
    println("ranking of common triple counts based on their count.")
    val sqlText3 = "SELECT  count CommonPredicates , COUNT(*) TriplesWithThisCommonPredicateNumber,  (Count * Count(*)) comparisonsRequired   FROM groupedSubjects group by COUNT ORDER BY COUNT(*) DESC"
    val rankedCounts = sparkSession.sql(sqlText3)
    rankedCounts.show(15, 80)
    rankedCounts
    /*
    For persons dataSet the result is

    ranking of common triple counts based on their count.
    +----------------+------------------------------------+-------------------+
    |CommonPredicates|TriplesWithThisCommonPredicateNumber|comparisonsRequired|
    +----------------+------------------------------------+-------------------+
    |               1|                             1500016|            1500016|
    |               4|                              226770|             907080|
    |               8|                              115928|             927424|
    |               7|                              101537|             710759|
    |               6|                               28255|             169530|
    |               3|                               21673|              65019|
    |               5|                                4079|              20395|
    |               2|                                1742|               3484|
    +----------------+------------------------------------+-------------------+

     */

  }

  /**
    * block triples based on matched predicates
    *
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

    //select from  "Subject1","Predicate1","Object1","Literal1", "Predicate3","Predicate4","Subject2","Predicate2","Object2","Literal2"

    val typeSubjectWithLiteral = samePredicateSubjectObjects.select("Subject1", "Literal1", "Subject2", "Literal2")
    //The other way round will be comparison of subjects. but if the URI format is hashed based then that is useless.
    // By comparing filtered objects we compare literals as well.
    // We cover two cases, when ids are embedded into URI and not literals, as in case of Drug bank data set
    // There is also a chance that entities from both kgs
    // refer to same KG using sameAs link.
    typeSubjectWithLiteral
  }

  def scheduleMatching(typeSubjectWithLiteral: DataFrame, memoryInGB: Integer): DataFrame = {


    typeSubjectWithLiteral.createOrReplaceTempView("sameTypes")
    println("the number of pairs of triples that are paired by matched predicate")
    val sqlText3 = "SELECT count(1) matchedPredicateTriplesSum  FROM sameTypes"
    val matchedPredicateTriplesSum = sparkSession.sql(sqlText3)
    matchedPredicateTriplesSum.show()


    val clusteredSubjects = this.clusterRankSubjects(typeSubjectWithLiteral)
    val clusterRankedCounts = this.clusterRankCounts(clusteredSubjects)

    val clusters = clusteredSubjects.toDF("Subject4", "Subject5", "commonPredicateCount")

    val numberOfCommonTriples = clusterRankedCounts.select("CommonPredicates", "comparisonsRequired")
       .rdd.map(r => (r(0).toString.toInt , r(1).toString.toInt )).sortByKey(false, 1).collect()

    val lengthOfNumberOfCommonTriples = numberOfCommonTriples.length


    //println(lengthOfNumberOfCommonTriples) //get the iteration number

    /*
     for 1 GigaByte memory, 40,000 pairs fits if more should be blocked
     For person1 dataset it was 4,303,707 and it gave "java.lang.OutOfMemoryError: GC overhead limit exceeded" error
     */
    val defaultMemoryForSlotSize = 1 //in GB
    val slotSize = 40000 // This is the maximum number of pairs that fit in 4 Gig heap memory
    val slots = memoryInGB / defaultMemoryForSlotSize


    var matchedUnion = matchedPredicateTriplesSum //just to inherit type of Data Frame

    for (x <- 1 to lengthOfNumberOfCommonTriples) {

      println("commonPredicate loop number " + x + " from " + lengthOfNumberOfCommonTriples)

      //var requiredSlots = matchedPredicateTriplesSum.collect.head(0).toString.toInt / slotSize
      var requiredSlots = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x )._2 / slotSize
      var requiredRepetition = 1
      if (requiredSlots > slots) requiredRepetition = requiredSlots / slots

      val commonPredicates = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x )._1.toString
      println("processing triples with number of commonPredicates equal to" + commonPredicates )

      var cluster = clusters.where(clusters("commonPredicateCount") === commonPredicates)

      var arr = Array.fill(requiredRepetition)(1.0)
      val clustersArray = cluster.randomSplit(arr)

      for (y <- 0 until requiredRepetition) {
        println("block loop number " + y + " from " + requiredRepetition)


        val firstMatchingLevel = typeSubjectWithLiteral.join(clustersArray(y),
          typeSubjectWithLiteral("subject1") === clusters("Subject4") &&
            typeSubjectWithLiteral("subject2") === clusters("Subject5")
           , "inner" //  && here Must filter a portion of slotSize fot each count and put that in a memory block
        )
        // it is better to start from count 1 and increase becasue they are more atomic and most probably are label or name
        // But can happen that many pairs have one common predicate so I should break them too and those that have a big number of comparison also are taking the memory.

        // block typeSubjectWithLiteral here based on clusteredSubjects
        // redistribute result of distribution such that gain cluster of equal size

        //suppose 10000 comparison fits in memory. start with the biggest one that has not less than

        //another case is that one block is very big. for example all the data is unified and had 8 links to compare. This
        // filtering method alone will not solve it. We have to break a big block to smaller ones.
        firstMatchingLevel.show(40, 80)

        if (x < 2) {
          var matched = getMatchedEntities(firstMatchingLevel)
          matchedUnion = matched
        } else {
          val matched = getMatchedEntities(firstMatchingLevel)
          matchedUnion = matchedUnion.union(matched)
        }
      }
    }
    matchedUnion
  }

  def getMatchedEntities(firstMatchingLevel: DataFrame): DataFrame = {
    val wordNetSim = new SimilarityHandler(0.5)
    val similarPairs = firstMatchingLevel.collect().map(x => (x.getString(0), x.getString(3),
      wordNetSim.areLiteralsEqual(x.getString(2), x.getString(5))))

    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val matched = rdd1.toDF("Subject1", "Subject2", "equal")

    matched.show(40)

    matched.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2 FROM matched where equal = true"
    val subjects = sparkSession.sql(sqlText1)
    subjects.show(50, 80)

    println("the number of matched pair of subjects that are paired by matched predicate and matched literals")
    val sqlText3 = "SELECT count(*) FROM matched where equal = true"
    val typedTriples3 = sparkSession.sql(sqlText3)
    typedTriples3.show()
    subjects
  }
}
