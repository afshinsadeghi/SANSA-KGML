package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
  * Created by afshin on 07.03.18.
  */
/**
  * @param sparkSession
  */

class Matching(sparkSession: SparkSession, simHandler : SimilarityHandler) extends EvaluationHelper {

  val similarityThreshold = 0.7
  var normedStringSimilarityThreshold = 0.7


  import sparkSession.sqlContext.implicits._

  var matchedUnion = Seq.empty[(String, String, String)].toDF("Subject1", "Subject2", "strSimilarity")
  var literalBasedClusterRankSubjects = Seq.empty[(String, String, String)].toDF("Subject1", "Subject2", "count")
  var subjectWithLiteral = Seq.empty[(String, String, String , String)].toDF("Subject1", "Literal1", "Subject2", "Literal2")
  val getURIEnding = udf((str: String) => {
    if (str.length > 0 && str.startsWith("<")) {
      try { //handling only URIs, ignoring literals
        var ending1 = str.split("<")(1).split(">")(0).split("/").last
        if (ending1.endsWith(" .")) ending1 = ending1.drop(2)
        ending1
      } catch {
        case e: Exception => null
      }
    } else {
      null
    }
  })

  /*
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
  */

  import org.apache.spark.sql.functions._

  /**
    * Clustering subjects based on common number of predicates that have literals.
    * The clusters are ranked and have no common pairs of subjects
    *
    * @param SubjectsWithLiteral
    * @return
    */
  def predicateBasedClusterRankSubjects(SubjectsWithLiteral: DataFrame): DataFrame = {

    val subjectsWithPredicate = SubjectsWithLiteral.drop("Literal1", "Literal2").dropDuplicates("Subject1", "Predicate1","Subject2", "Predicate2") //a subject and predicate can have several different literals

    subjectsWithPredicate.createOrReplaceTempView("sameTypes")

    val sqlText2 = "SELECT  subject1, subject2, COUNT(*) as count FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
    val blockedSubjects2 = sparkSession.sql(sqlText2)
    if (printReport) {
      println("Ranking of subjects based on their common predicate")
      blockedSubjects2.show(15, 80)
    }
    blockedSubjects2
  }
    /**
      * Clustering subjects based on common number of predicates that have literals.
      * The clusters are ranked and have no common pairs of subjects
      *
      * @param SubjectsWithLiteral
      * @return
      */
    def literalBasedClusterRankSubjects(SubjectsWithLiteral: DataFrame): DataFrame = {

      SubjectsWithLiteral.createOrReplaceTempView("sameTypes")//a subject and predicate can have several different literals

      val sqlText2 = "SELECT  subject1, subject2, COUNT(*) as count FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
      val blockedSubjects2 = sparkSession.sql(sqlText2)
      if(printReport) {
        println("Ranking of subjects based on their common predicate and objects)")
        blockedSubjects2.show(15, 80)
      }
      blockedSubjects2
    }
    /*
    Result of literalBasedClusterRankSubjects for drugbank
+-----------------------------------------------------------------+--------------------------------------------------------------+--------+
|                                                         subject1|                                                      subject2|count(1)|  this is without using drop
+-----------------------------------------------------------------+--------------------------------------------------------------+--------+
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00112>|                            <http://dbpedia.org/resource/2C-B>|    1268| this is number of literals not predicates
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


  def clusterRankCounts(rankedSubjectWithCommonPredicateCount: DataFrame): DataFrame = {

   val rankedCounts =  if (printReport) {
      rankedSubjectWithCommonPredicateCount.createOrReplaceTempView("groupedSubjects")
      val sqlText3 = "SELECT  count CommonPredicates , COUNT(*) TriplesWithThisCommonPredicateNumber,  (Count * Count(*)) comparisonsRequired   FROM groupedSubjects group by COUNT ORDER BY COUNT(*) DESC"
      val rankedCounts = sparkSession.sql(sqlText3)
      println("ranking of common triple counts based on their count.")
      rankedCounts.show(15, 80)
       rankedCounts
    }else{ // optimized version of clusterRankCounts for speed : optimized version for speed reduce calculation of column comparisonsRequired
     rankedSubjectWithCommonPredicateCount.createOrReplaceTempView("groupedSubjects")
      val sqlText3 = "SELECT  count CommonPredicates , COUNT(*) TriplesWithThisCommonPredicateNumber  FROM groupedSubjects group by COUNT ORDER BY COUNT DESC"
      val rankedCounts = sparkSession.sql(sqlText3)
     // rankedCounts.show(15, 80)
      rankedCounts
    }
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

  def muxPartitions[T: ClassTag](rdd: RDD[T], n: Int, f: (Int, Iterator[T]) => Seq[T],
                                 persist: StorageLevel): Seq[RDD[T]] = {
    val mux = rdd.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => Iterator.single(itr.next()(j)) } }
  }

  def flatMuxPartitions[T: ClassTag](rdd: RDD[T], n: Int, f: (Int, Iterator[T]) => Seq[TraversableOnce[T]],
                                     persist: StorageLevel): Seq[RDD[T]] = {
    val mux = rdd.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => itr.next()(j).toIterator } }
  }

  import org.apache.spark.storage.StorageLevel._

  def splitSampleMux[T: ClassTag](rdd: RDD[T], n: Int,
                                  persist: StorageLevel = MEMORY_ONLY,
                                  seed: Long = 42): Seq[RDD[T]] =
    this.flatMuxPartitions(rdd, n, (id: Int, data: Iterator[T]) => {
      scala.util.Random.setSeed(id.toLong * seed)
      val samples = Vector.fill(n) {
        scala.collection.mutable.ArrayBuffer.empty[T]
      }
      data.foreach { e => samples(scala.util.Random.nextInt(n)) += e }
      samples
    }, persist)

  /**
    * It considers both WordNet match of URIS and children match and performs a Jaccard sim on all Matched and not matched children
    * When we go up one level, we assumed all the children of the parent are already compared.
    * @param parentNodesWithLiteral
    * @return
    */
  def scheduleParentMatching( parentNodesWithLiteral: DataFrame) : DataFrame = {


    var matched = getMatchedEntityPairsBasedOnLiteralSim(parentNodesWithLiteral)
    //matched = matched.union(childSubjectsMatch)
    normedStringSimilarityThreshold = 0.93
    val localLiteralBasedClusterRankSubjects = literalBasedClusterRankSubjects(matched) //count matched items a new relation, todo:test it in practice. if the predicate is matched it is being counted already
    this.aggregateMatchedEntities(matched, localLiteralBasedClusterRankSubjects) //must have old sim value
    matched = matched.filter(col("Subject1") =!= col("Subject2")) //matchedUnion.union
    matchedUnion = matchedUnion.union(matched).dropDuplicates("Subject1","Subject2")
    matchedUnion.persist()
  }
  /**
    *
    * @param typeSubjectWithLiteral
    * @param memoryInGB
    * @return
    */
  def scheduleLeafMatching(typeSubjectWithLiteral: DataFrame, memoryInGB: Integer): DataFrame = {

   /*
    subjectWithLiteral.createOrReplaceTempView("sameTypes")
      println("the number of pairs of triples that are paired by matched predicate")
      val sqlText3 = "SELECT count(1) matchedPredicateTriplesSum  FROM sameTypes"
      val matchedPredicateTriplesSum = sparkSession.sql(sqlText3)
      //var requiredSlots = matchedPredicateTriplesSum.collect.head(0).toString.toInt / slotSize

      matchedPredicateTriplesSum.show()
*/
    subjectWithLiteral = typeSubjectWithLiteral.drop("Predicate1", "Predicate2")
    val clusteredSubjects = this.predicateBasedClusterRankSubjects(typeSubjectWithLiteral)
    val cores = Runtime.getRuntime.availableProcessors
    val clusterRankedCounts = this.clusterRankCounts(clusteredSubjects).repartition(cores * 3)
    val clusters = clusteredSubjects.toDF("Subject4", "Subject5", "commonPredicateCount")

    val numberOfSameCommonTriples = if (printReport) {
     val numberOfSameCommonTriples = clusterRankedCounts.select("CommonPredicates", "comparisonsRequired")
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt)).sortByKey(false, 1).collect()

      println("Blocking schedule: ")
      print(numberOfSameCommonTriples.map(_.toString().mkString).mkString("\n"))
      println()
      numberOfSameCommonTriples
    }else{
     val   numberOfSameCommonTriples = clusterRankedCounts.select("CommonPredicates")
        .rdd.map(r => (r(0).toString.toInt,r(0).toString.toInt )).sortByKey(false, 1).collect()

      println("Blocking schedule: ")   //works with optimized version for speed without calculation of column comparisonsRequired
      print(numberOfSameCommonTriples.map(_._1.toString().mkString).mkString("\n"))
      println()
      numberOfSameCommonTriples
    }

    val lengthOfNumberOfSameCommonTriples = numberOfSameCommonTriples.length


    //println(lengthOfNumberOfSameCommonTriples) //get the iteration number

    /*
     for 1 GigaByte memory, 40,000 pairs fits if more should be blocked
     For person1 dataset it was 4,303,707 and it gave "java.lang.OutOfMemoryError: GC overhead limit exceeded" error
     */
    val defaultMemoryForSlotSize = 1 //in GB
    val slotSize = 40000 // This is the maximum number of pairs that fit in 4 Gig heap memory
    val slots = memoryInGB / defaultMemoryForSlotSize

    //val schema1 = new StructType()
    //  .add(StructField("Subject1", StringType, true))
    //  .add(StructField("Subject2", StringType, true))


    // import sparkSession.sqlContext.implicits._
    // val matchedEmptyDF = Seq.empty[(String, String, String)].toDF("Subject1", "Subject2", "strSimilarity")
    // var matchedUnion = matchedEmptyDF //just to inherit type of Data Frame

    var last = 1
    if (lengthOfNumberOfSameCommonTriples > 1) last = 2 // for better precision ignore comparing those who has only one common predicate
    //The loop starts with those who have the most common predicates and if they match, do not compare them with other entities
    for (x <- lengthOfNumberOfSameCommonTriples to last by -1) {

      println("commonPredicate subjects loop number " + x + " from " + lengthOfNumberOfSameCommonTriples)

//      val requiredSlots = numberOfSameCommonTriples(lengthOfNumberOfSameCommonTriples - x)._2 / slotSize
//      var requiredSplit = 1
      //if (requiredSlots > slots) requiredSplit = requiredSlots / slots

      val commonPredicates = numberOfSameCommonTriples(lengthOfNumberOfSameCommonTriples - x)._1.toString
      println("In bigger loop: processing triples with number of commonPredicates equal to " + commonPredicates)

      var cluster =
        clusters.where(clusters("commonPredicateCount") === commonPredicates)

      //var matched =
      this.matchCluster(cluster)
      //this.matchCluster(subjectWithLiteral, cluster)

      //this.matchACluster(requiredSplit, subjectWithLiteral, cluster)
      //this.matchAClusterOptimized(requiredSplit, subjectWithLiteral, cluster)
      //   if (x == lengthOfNumberOfSameCommonTriples) { //matchedUnion is empty
      //     this.matchAClusterOptimized(requiredSplit, subjectWithLiteral, cluster)
      //     //matchedUnion.persist()
      //    }
      //    else {
      //       this.matchAClusterOptimized(requiredSplit, subjectWithLiteral, cluster)
      //       //matchedUnion.persist()
      //     }
    }
    if (printReport) {
      println("finding pair sums...")
    }
    literalBasedClusterRankSubjects = this.literalBasedClusterRankSubjects(typeSubjectWithLiteral)
    //literalBasedClusterRankSubjects = clusteredSubjects
    val uniqueLiteralMatchedSubjects = aggregateMatchedEntities(matchedUnion, literalBasedClusterRankSubjects)
    uniqueLiteralMatchedSubjects
  }

  def matchCluster(cluster: DataFrame): Unit = {

    //val subjectWithLiteral = typeSubjectWithLiteral.toDF("Subject1", "Literal1", "Subject2", "Literal2")

      val firstMatchingLevel = subjectWithLiteral
        .join(cluster.drop("commonPredicateCount").except(matchedUnion.drop("strSimilarity")),
            subjectWithLiteral("subject1") === cluster("Subject4") &&
            subjectWithLiteral("subject2") === cluster("Subject5")
        , "inner" //  && here filter a portion of slotSize for each count and put that in a memory block
      ).select("Subject1", "Literal1", "Subject2", "Literal2")
     // do not need to split, no collect is used any more.
      if (printReport) {
        firstMatchingLevel.show(40, 80)
      }
      var matched = getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel)
      matchedUnion = matchedUnion.union(matched).persist()
  }

/*
  def matchAClusterOptimized(requiredSplit: Int, typeSubjectWithLiteral: DataFrame,
                             cluster: DataFrame): Unit = {

    val subjectWithLiteral = typeSubjectWithLiteral.toDF("Subject3", "Literal1", "Subject4", "Literal2")
    println("requiredSplit:" + requiredSplit)
    //var localUnion = matchedEmptyDF //just to inherit type of Data Frame
    val clustersSeq = this.splitSampleMux(cluster.rdd, requiredSplit, MEMORY_ONLY, 0)
    val schema = new StructType()
      .add(StructField("Subject1", StringType, true))
      .add(StructField("Subject2", StringType, true))
      .add(StructField("commonPredicateCount", LongType, true))
    var counter = 1
    clustersSeq.foreach(a => {
      println("In parallel cluster number " + counter + " from " + requiredSplit)
      val b = sparkSession.createDataFrame(a, schema).except(matchedUnion)
      //.where(col("Subject4") =!= matchedUnion.col("Subject1") || col("Subject5") =!= matchedUnion.col("Subject2"))
      val firstMatchingLevel =
     //   if (matchedUnion.rdd.isEmpty()) {
          subjectWithLiteral.join(
            b,
            subjectWithLiteral("subject3") === b("Subject1") &&
              subjectWithLiteral("subject4") === b("Subject2")
            , "inner").select("Subject1", "Literal1", "Subject2", "Literal2")
     //   } else {
     //     subjectWithLiteral.join(
     //       b,
     //       subjectWithLiteral("subject3") === b("Subject1") &&
     //         subjectWithLiteral("subject4") === b("Subject2")
     //       , "inner").select("Subject1", "Literal1", "Subject2", "Literal2")
    //    }

      //  && here Must filter a portion of slotSize for each count and put that in a memory block
      if (printReport) {
        println("firstMatchingLevel: matching subjects pairs based on literals of common predicates")
        firstMatchingLevel.show(40, 80)
      }
      var matched = getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel)
      if (printReport) {
        println("matched: ")
        matched.show(40, 80)
      }
      //localUnion = localUnion.union(matched)
      matchedUnion = matchedUnion.union(matched).persist()
      counter = counter + 1
    })

  }
*/
/*
    def matchACluster(requiredRepetition: Int, typeSubjectWithLiteral: DataFrame,
                      cluster: DataFrame): Unit = {

      //var localUnion = matchedEmptyDF //just to inherit type of Data Frame

      var arr = Array.fill(requiredRepetition)(1.0 / requiredRepetition)
      val clustersArray = cluster.randomSplit(arr)

      for (y <- 0 until requiredRepetition) {
        println("block loop number " + y + 1 + " from " + requiredRepetition)


        val firstMatchingLevel = typeSubjectWithLiteral.join(clustersArray(y),
          typeSubjectWithLiteral("subject1") === clustersArray(y)("Subject4") &&
            typeSubjectWithLiteral("subject2") === clustersArray(y)("Subject5")
          , "inner" //  && here Must filter a portion of slotSize fot each count and put that in a memory block
        )
        // it is better to start from count 1 and increase becasue they are more atomic and most probably are label or name
        // But can happen that many pairs have one common predicate so I should break them too and those that have a big number of comparison also are taking the memory.

        // block typeSubjectWithLiteral here based on clusteredSubjects
        // redistribute result of distribution such that gain cluster of equal size

        //suppose 10000 comparison fits in memory. start with the biggest one that has not less than

        //another case is that one block is very big. for example all the data is unified and had 8 links to compare. This
        // filtering method alone will not solve it. We have to break a big block to smaller ones.
        if (printReport) {
          firstMatchingLevel.show(40, 80)
        }
        var matched = getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel)
        //if (y == 0) localUnion = matched
        //else localUnion = localUnion.union(matched)
        matchedUnion = matchedUnion.union(matched).persist()
      }
      //localUnion
    }
*/

  /**
    *
    * @param firstMatchingLevel
    * @return
    */
  def getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel: DataFrame): DataFrame = {
    // val similarPairs = firstMatchingLevel.collect().map(x => (x.getString(0), x.getString(2),
    // simHandler.getSimilarity(x.getString(1), x.getString(3))))
    simHandler.setThreshold(similarityThreshold)

    var subjectsComparedByLiteral  = firstMatchingLevel // just to initialize
    //wordnet and exact match is just for running experiments.
    if (wordNetMatchEvaluation){
       subjectsComparedByLiteral = firstMatchingLevel.where(col("Literal1").isNotNull && col("Literal2").isNotNull)
        .withColumn("strSimilarity", simHandler.getWordNetSimilarityUDF(col("Literal1"), col("Literal2")))

    }
    else if(exactMatchEvaluation){
       subjectsComparedByLiteral = firstMatchingLevel.where(col("Literal1").isNotNull && col("Literal1") === col("Literal2"))

    }else{
       subjectsComparedByLiteral = firstMatchingLevel.where(col("Literal1").isNotNull && col("Literal2").isNotNull)
      .withColumn("strSimilarity", simHandler.getSimilarityUDF(col("Literal1"), col("Literal2")))
    }

    // val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    //  import sparkSession.sqlContext.implicits._
    //   val subjectsComparedByLiteral = rdd1.toDF("Subject1", "Subject2", "strSimilarity")
    if (printReport) {
      println("Number of compared pairs:" + subjectsComparedByLiteral.count())
      subjectsComparedByLiteral.show(40, 80)
    }
    subjectsComparedByLiteral.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2, strSimilarity FROM matched WHERE strSimilarity > " + similarityThreshold.toString
    val matchedSubjects = sparkSession.sql(sqlText1) //.persist()
    if (printReport) {
      matchedSubjects.show(50, 80)
      println("the number of matched pair of subjects that are paired by matched predicate and matched literals")
      val sqlText3 = "SELECT count(*) FROM matched where strSimilarity > " + similarityThreshold.toString
      val typedTriples3 = sparkSession.sql(sqlText3)
      typedTriples3.show()
    }
    matchedSubjects //filtered once by literal comparison
  }


  /**
    * I collect sum of similarity for each pair of subject1 and subject2 sum the similarity of all their literals and make a norm of them.
    * those literals that are more than the threshold
    * so here we have unique pairs of subject1 and subject2
    *
    * @param unionMatched
    * @return
    */
  def aggregateMatchedEntities(unionMatched: DataFrame, clusteredSubjects: DataFrame): DataFrame = {

    unionMatched.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2, SUM(strSimilarity) FROM matched Group By Subject1, Subject2"
    val matchedSubjectsResult = sparkSession.sql(sqlText1)

    val matchedSubjects = matchedSubjectsResult.toDF("Subject3", "Subject4", "sumStrSimilarity")

    val matchedSubjectsJoined = matchedSubjects.join(clusteredSubjects, matchedSubjects("Subject3") ===
      clusteredSubjects("Subject1") && matchedSubjects("Subject4") === clusteredSubjects("Subject2"))
    matchedSubjectsJoined.createOrReplaceTempView("matchedJoined1")
    val sqlText2 = "SELECT Subject1, Subject2,  sumStrSimilarity/count normStrSim FROM matchedJoined1" //its Jaccard of children sim as in http://afshn.com/papers/Sadeghi_SCM-KG.pdf
    val pairedSubjectsWithNormSim = sparkSession.sql(sqlText2)


    pairedSubjectsWithNormSim.createOrReplaceTempView("pairedSubjects")
    val sqlText3 = "SELECT Subject1, Subject2, normStrSim FROM pairedSubjects WHERE Subject1 != Subject2 AND normStrSim > " + normedStringSimilarityThreshold.toString
    val matchedSubjectsWithNormSim = sparkSession.sql(sqlText3)

    if (printReport) {
      matchedSubjectsWithNormSim.show(50, 80)

      matchedSubjectsWithNormSim.createOrReplaceTempView("matchedLitNormed")
      println("The number of unique matched pair of subjects that are paired by matched predicate and matched literals")
      val sqlText3 = "SELECT count(*) From matchedLitNormed "
      val typedTriples3 = sparkSession.sql(sqlText3)
      typedTriples3.show()
    }
    matchedSubjectsWithNormSim //filtered once by literal comparison
  }

  /**
    * They are two type of non-literal objects.
    * those who do not apear as an subject. Their similarity must be calculated with simple wordnet.
    * for those that apear in subject I do molecular similarity from paper http://afshn.com/papers/Sadeghi_SCM-KG.pdf
    *
    */
  /* removed this function because it I found it more optimal to run Wordnet at blocking time
  def matchNonLiteralSubjectsBasedOnWordNet(df1: DataFrame, df2: DataFrame, subjectsComparedByLiteral: DataFrame, matchedPredicates: DataFrame): DataFrame = {
    //from matchedSubjects fetch those objects that do not appear as subject in other triples

    df1.createOrReplaceTempView("triple")
    df2.createOrReplaceTempView("triple2")
    matchedPredicates.createOrReplaceTempView("predicates")
    subjectsComparedByLiteral.createOrReplaceTempView("subjectStrPaired")

    val sqlText1 = "SELECT" +
      " triple.Subject1, triple.predicate1, triple.object1, triple2.Subject2 ,  triple2.predicate2," +
      " triple2.object2, subjectStrPaired.Subject1 " +
      "           FROM triple , triple2, subjectStrPaired , predicates " +
      "WHERE triple.subject1 = AND subjectStrPaired.Subject1 AND triple2.subject2 = subjectStrPaired.Subject2 AND" +
      "triple.predicate1 = predicates.predicate1 AND triple2.predicate2 = predicates.predicate2"
    println("subject pairs to compare their non-literal objects")
    val subjectPairs = sparkSession.sql(sqlText1)
    subjectPairs.show()

    // "triple.Predicate1 = predicates.predicate1 AND triple2.predicate2 = predicates.Predicate2"
    subjectPairs
    val dF1 = df1.
      withColumn("object1e", getURIEnding(col("object1")))

    var dF2 = df2.
      withColumn("object2e", getURIEnding(col("object2")))

    subjectPairs
  }

*/
  /**
    *1. after the column based literal process we used string based similarity in the function above,
    * we use that similarity (can be sum of sims translated to probabilites).
    *2. instead of fetching all triples, we filtered  subject1 to a column to subject2 by comparison of literals
    *3. here we do wordnet comparison on entities, both (subject later) and object which is URI based
    * *
    * Match from the dataset of all triples those triples with objects that are URI and their subject is filtered
    * in the step above (getMatchedEntitiesBasedOnLiteralSim)
    */
  /*
  def matchdEntitiesBasedOnWordNet(df1: DataFrame,df2 : DataFrame,  subjectsComparedByLiteral: DataFrame, matchedPredicates: DataFrame): DataFrame = {

    //join df1 and df2 with triples that have subject1 and subject2 and those predicate1 and predicate2 from matchedPredicates

  //val predicatesPairs = matchedPredicates.toDF("Predicate3", "Predicate4")

    val dF1 = df1. withColumn("subject1e", getURIEnding(col("subject1"))).
      withColumn("object1e", getURIEnding(col("object1")))

    var dF2 = df2.withColumn("subject2e", getURIEnding(col("subject2"))).
      withColumn("object2e", getURIEnding(col("object2")))

      dF1.createOrReplaceTempView("triple")
      dF2.createOrReplaceTempView("triple2")
    matchedPredicates.createOrReplaceTempView("predicates")
    subjectsComparedByLiteral.createOrReplaceTempView("subjectStrPaired")

    //todo:finish this function to match those leaf non literal objects .
    // sending a big data set to functions may not efficent, maybe better test its usage is benefitical.
    // for a big data set I suspect this would be .
    //Also I suspect I can fetch this portion of objects in the scheduler itself.
    //I would say I could this : In the big RDD if the object is literal do string compare, if it is non-literal do a wordnet compare. but the problem is putting an if on each row make it very slow. I try it

    //val samePredicateSubjectObjects = dF1.
   // join(predicatesPairs, dF1("predicate1") <=> predicatesPairs("Predicate3")).
   // join(dF2, predicatesPairs("Predicate4") <=> dF2("predicate2") &&
   //   dF1("subject1ending").isNotNull && dF1("subject1ending").isNotNull && dF2("object2ending").isNotNull && dF2("object2ending").isNotNull)

    // |            Subject1|          Predicate1|             Object1|   subject1ending|       object1ending|          Predicate3|          Predicate4|
    //         Subject2|          Predicate2|             Object2|     subject2ending|       object2ending|
//using sparsql instead to only select  columns needed:

    val sqlText1 = "SELECT triple.Subject1e, triple2.Subject2e FROM triple , triple2,  predicates, subjectStrPaired WHERE" +
      " triple.subject1 = subjectStrPaired.Subject1 AND triple2.subject2 = subjectStrPaired.Subject2 AND " +
      "triple.Predicate1 = predicates.predicate1 AND triple2.predicate2 = predicates.Predicate2"
   // val sqlText1 = "SELECT Subject1, Object1 FROM triple , Subject2, Object2 FROM triple2 WHERE Predicate1 = Predicate3 AND Predicate2 = Predicate4"
    println("subject pairs to match")
    val subjectPairs = sparkSession.sql(sqlText1)
    subjectPairs.show()

    subjectPairs


    if (printReport) {
        samePredicateSubjectObjects.show(60, 20)
    }
   // then perform wordnet sim on subject ending and object ending

  val wordNetSimilarityThreshold = 0.5
  val simHandler = new SimilarityHandler(wordNetSimilarityThreshold)
  val similarPairs = samePredicateSubjectObjects.collect().map(x => (x.getString(0), x.getString(1),
  simHandler.jaccardLiteralSimilarityWithLevenshtein(x.getString(1), x.getString(3)),
  simHandler.jaccardPredicateSimilarityWithWordNet(x.getString(0), x.getString(2))))


   //todo:apply wordnet on subjects and objects pairs
  val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
  import sparkSession.sqlContext.implicits._
  val subjectsComparedByWordNet = rdd1.toDF("Subject1", "Subject2", "wrdSimilarity")
  if (printReport) {
  subjectsComparedByWordNet.show(40)
}

  subjectsComparedByWordNet.createOrReplaceTempView("matched")
  val sqlText1 = "SELECT Subject1, Subject2 FROM matched WHERE wrdSimilarity > " + wordNetSimilarityThreshold.toString
  val matchedSubjects = sparkSession.sql(sqlText1)
  if (printReport) {
  matchedSubjects.show(50, 80)
}
  println("the number of matched pair of subjects that are paired by matched predicate and matched literals")
  val sqlText2 = "SELECT count(*) FROM matched where wrdSimilarity > " + wordNetSimilarityThreshold.toString
  val typedTriples2 = sparkSession.sql(sqlText2)
  typedTriples2.show()

    subjectsComparedByWordNet
  }
     */
  //todo:repeat getMatchedEntitiesBasedOnSumLiteralSim function for non literals, transitively to find deeper levels of entities to match

  //rank the predicate and literals that are repeated the most. and give them less similarity point


}