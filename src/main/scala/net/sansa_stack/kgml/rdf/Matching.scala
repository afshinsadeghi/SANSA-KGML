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
class Matching(sparkSession: SparkSession) extends EvaluationHelper {

  val wordNetPredicateMatch = udf((S: String, S2: String) => {

    var ending1 = S.split("<")(1).split(">")(0)
    var ending2 = S2.split("<")(1).split(">")(0)
    val wordNetSim = new SimilarityHandler(0.7)
    wordNetSim.arePredicatesEqual(ending1, ending2)
  })

  /**
    * get literal value in objects
    */
  val getLiteralValue = udf((S: String) => {
    //println("input:" + S)
    if (S == null) null
    else if (S.length > 0 && S.startsWith("<")) {
      try { //handling special case of Drugbank that puts casRegistryName in URIs, matching between literlas and uri
        //   var str = S.split("<")(1).split(">")(0).split("/").last
        //  if (str.endsWith(" .")) str = str.drop(2)
        // str
        null
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
    *     Clustering subjects based on common number of predicates that have literals.
    *     The clusters are ranked and have no common pairs of subjects
    * @param SubjectsWithLiteral
    * @return
    */
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
    *
    * @param typeSubjectWithLiteral
    * @param memoryInGB
    * @return
    */
  def scheduleLiteralBasedMatching(typeSubjectWithLiteral: DataFrame, memoryInGB: Integer): DataFrame = {


    typeSubjectWithLiteral.createOrReplaceTempView("sameTypes")
    println("the number of pairs of triples that are paired by matched predicate")
    val sqlText3 = "SELECT count(1) matchedPredicateTriplesSum  FROM sameTypes"
    val matchedPredicateTriplesSum = sparkSession.sql(sqlText3)
    matchedPredicateTriplesSum.show()


    val clusteredSubjects = this.clusterRankSubjects(typeSubjectWithLiteral)
    val clusterRankedCounts = this.clusterRankCounts(clusteredSubjects)

    val clusters = clusteredSubjects.toDF("Subject4", "Subject5", "commonPredicateCount")

    val numberOfCommonTriples = clusterRankedCounts.select("CommonPredicates", "comparisonsRequired")
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt)).sortByKey(false, 1).collect()

    if (printReport) {
      println("Blocking schedule: ")
      print(numberOfCommonTriples.map(_.toString().mkString).mkString("\n"))
      println()
    }

    val lengthOfNumberOfCommonTriples = numberOfCommonTriples.length


    //println(lengthOfNumberOfCommonTriples) //get the iteration number

    /*
     for 1 GigaByte memory, 40,000 pairs fits if more should be blocked
     For person1 dataset it was 4,303,707 and it gave "java.lang.OutOfMemoryError: GC overhead limit exceeded" error
     */
    val defaultMemoryForSlotSize = 1 //in GB
    val slotSize = 40000 // This is the maximum number of pairs that fit in 4 Gig heap memory
    val slots = memoryInGB / defaultMemoryForSlotSize

    val schema1 = new StructType()
      .add(StructField("Subject1", StringType, true))
      .add(StructField("Subject2", StringType, true))


    import sparkSession.sqlContext.implicits._
    val matchedEmptyDF = Seq.empty[(String, String, String)].toDF("Subject1", "Subject2", "strSimilarity")
    var matchedUnion = matchedEmptyDF //just to inherit type of Data Frame

    for (x <- 1 to lengthOfNumberOfCommonTriples) {

      //println("commonPredicate subjects loop number " + x + " from " + lengthOfNumberOfCommonTriples)

      //var requiredSlots = matchedPredicateTriplesSum.collect.head(0).toString.toInt / slotSize
      var requiredSlots = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x)._2 / slotSize
      var requiredRepetition = 1
      if (requiredSlots > slots) requiredRepetition = requiredSlots / slots

      val commonPredicates = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x)._1.toString
      println("In bigger loop: processing triples with number of commonPredicates equal to " + commonPredicates)

      var cluster =
        clusters.where(clusters("commonPredicateCount") === commonPredicates)

      //var matched = this.matchACluster(requiredRepetition, typeSubjectWithLiteral, cluster, matchedEmptyDF)
      var matched =
        this.matchAClusterOptimized(requiredRepetition, typeSubjectWithLiteral, cluster, matchedEmptyDF)

      if (x == 1) {
        matchedUnion = matched
        matchedUnion.cache()
      }
      else {
        matchedUnion = matchedUnion.union(matched)
        matchedUnion.cache()
      }
    }
    if (printReport) {
      println("finding pair sums...")
    }
    var uniqueLiteralMatchedSubjects = getMatchedEntitiesBasedOnSumLiteralSim(matchedUnion, clusteredSubjects)
    uniqueLiteralMatchedSubjects
  }

  /**
    *
    * @param requiredRepetition
    * @param typeSubjectWithLiteral
    * @param cluster
    * @param matchedEmptyDF
    * @return
    */

  def matchAClusterOptimized(requiredRepetition: Int, typeSubjectWithLiteral: DataFrame,
                             cluster: DataFrame, matchedEmptyDF: DataFrame): DataFrame = {


    var localUnion = matchedEmptyDF //just to inherit type of Data Frame
    val clustersSeq = this.splitSampleMux(cluster.rdd, requiredRepetition, MEMORY_ONLY, 0)
    val schema = new StructType()
      .add(StructField("Subject4", StringType, true))
      .add(StructField("Subject5", StringType, true))
      .add(StructField("commonPredicateCount", LongType, true))
    var counter = 1
    clustersSeq.foreach(a => {
      val b = sparkSession.createDataFrame(a, schema)
      val firstMatchingLevel = typeSubjectWithLiteral.join(b,
        typeSubjectWithLiteral("subject1") === b("Subject4") &&
          typeSubjectWithLiteral("subject2") === b("Subject5")
        , "inner") //  && here Must filter a portion of slotSize fot each count and put that in a memory block
      if (printReport) {
        println("firstMatchingLevel: machting subjects pairs based on literals of common predicates")
        firstMatchingLevel.show(40, 80)
      }
      var matched = getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel)
      if (printReport) {
        println("matched: ")
        matched.show(40, 80)
      }
      localUnion = localUnion.union(matched)
      println("In parallel cluster number " + counter + " from " + requiredRepetition)
      counter = counter + 1
    })
    localUnion
  }

  /*
    def matchACluster(requiredRepetition: Int, typeSubjectWithLiteral: DataFrame,
                      cluster: DataFrame, matchedEmptyDF: DataFrame): DataFrame = {

      var localUnion = matchedEmptyDF //just to inherit type of Data Frame

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
        var matched = getMatchedEntitiesBasedOnLiteralSim(firstMatchingLevel)
        if (y == 0) localUnion = matched
        else localUnion = localUnion.union(matched)
      }
      localUnion
    }
  */

  /**
    *
    * @param firstMatchingLevel
    * @return
    */
  def getMatchedEntityPairsBasedOnLiteralSim(firstMatchingLevel: DataFrame): DataFrame = {
    val stringSimilarityThreshold = 0.7
    val simHandler = new SimilarityHandler(stringSimilarityThreshold)
    val similarPairs = firstMatchingLevel.collect().map(x => (x.getString(0), x.getString(2),
      simHandler.jaccardLiteralSimilarityWithLevenshtein(x.getString(1), x.getString(3))))

    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val subjectsComparedByLiteral = rdd1.toDF("Subject1", "Subject2", "strSimilarity")
    if (printReport) {
      subjectsComparedByLiteral.show(40)
    }
    subjectsComparedByLiteral.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2, strSimilarity FROM matched WHERE strSimilarity > " + stringSimilarityThreshold.toString
    val matchedSubjects = sparkSession.sql(sqlText1)
    if (printReport) {
      matchedSubjects.show(50, 80)
      println("the number of matched pair of subjects that are paired by matched predicate and matched literals")
      val sqlText3 = "SELECT count(*) FROM matched where strSimilarity > " + stringSimilarityThreshold.toString
      val typedTriples3 = sparkSession.sql(sqlText3)
      typedTriples3.show()
    }
    matchedSubjects //filtered once by literal comparison
  }


  /**
    * I collect sum of similarity for each pair of subject1 and subject2 sum the similairty of all their literls.
    * those literals that are more than the threshold
    * so here we have unique pairs of subject1 and subject2
    *
    * @param unionMatched
    * @return
    */
  def getMatchedEntitiesBasedOnSumLiteralSim(unionMatched: DataFrame, clusteredSubjects :DataFrame) : DataFrame = {

    unionMatched.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2, SUM(strSimilarity) FROM matched Group By Subject1, Subject2"
    val matchedSubjectsResult = sparkSession.sql(sqlText1)

    val matchedSubjects = matchedSubjectsResult.toDF("Subject3", "Subject4", "sumStrSimilarity")
    val normedStringSimilarityThreshold = 0.7

    val matchedSubjectsJoined = matchedSubjects.join(clusteredSubjects, matchedSubjects("Subject3") ===
      clusteredSubjects("Subject1") && matchedSubjects("Subject4") === clusteredSubjects("Subject2"))
    matchedSubjectsJoined.createOrReplaceTempView("matchedJoined1")
    val sqlText2 = "SELECT Subject1, Subject2,  sumStrSimilarity/count normStrSim FROM matchedJoined1"
    val pairedSubjectsWithNormSim = sparkSession.sql(sqlText2)

    pairedSubjectsWithNormSim.createOrReplaceTempView("pairedSubjects")
    val sqlText3 = "SELECT Subject1, Subject2, normStrSim FROM pairedSubjects  WHERE normStrSim > " + normedStringSimilarityThreshold
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
  1. after the column based literal process we used string based similarity in the function above,
     we use that similarity (can be sum of sims translated to probabilites).
  2. instead of fetching all triples , we filtered  subject1 to a column to subject2 by comparison of literals
  3. here we do wordnet comparison on entities, both subject and object which is URI based
     and check if a,b,c and a2,b,c exist
   Match from the dataset of all triples those triples with objects that are URI and their subject is filtered
      in the step above (getMatchedEntitiesBasedOnLiteralSim)
    * @param df1
    * @param df2
    * @param subjectsComparedByLiteral
    * @return
    */
  def matchdEntitiesBasedOnWordNet(df1: DataFrame,df2 : DataFrame,  subjectsComparedByLiteral: DataFrame, matchedPredicates: DataFrame): DataFrame = {

  val dF1 = df1. withColumn("subject1ending", getURIEnding(col("subject1"))).
    withColumn("object1ending", getURIEnding(col("object1")))

  var dF2 = df2.withColumn("subject2ending", getURIEnding(col("subject2"))).
    withColumn("object2ending", getURIEnding(col("object2")))

    //join df1 and df2 with triples that have subject1 and subject2 and those predicate1 and predicate2 from matchedPredicates

  val predicatesPairs = matchedPredicates.toDF("Predicate3", "Predicate4")
  dF1.createOrReplaceTempView("triple")
  dF2.createOrReplaceTempView("triple2")
  val samePredicateSubjectObjects = dF1.
    join(predicatesPairs, dF1("predicate1") <=> predicatesPairs("Predicate3")).
    join(dF2, predicatesPairs("Predicate4") <=> dF2("predicate2") &&
      dF2("subject1ending").isNotNull && dF1("subject2ending").isNotNull && dF2("object1ending").isNotNull && dF1("object2ending").isNotNull)


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
//todo:repeat getMatchedEntitiesBasedOnSumLiteralSim function for non literals, transitively to find deeper levels of entities to match

  //rank the predicate and literals that are repeated the most. and give them less similarity point


}