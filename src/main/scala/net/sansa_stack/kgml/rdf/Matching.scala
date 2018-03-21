package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.math.min
import scala.reflect.ClassTag

/**
  * Created by afshin on 07.03.18.
  */
class Matching(sparkSession: SparkSession) extends EvaluationHelper {

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


  def getURIEnding(str: String): String = {
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
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt)).sortByKey(false, 1).collect()

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
    val matchedEmptyDF = Seq.empty[(String, String)].toDF("Subject1", "Subject2")
    var matchedUnion = matchedEmptyDF //just to inherit type of Data Frame

    for (x <- 1 to lengthOfNumberOfCommonTriples) {

      //println("commonPredicate subjects loop number " + x + " from " + lengthOfNumberOfCommonTriples)

      //var requiredSlots = matchedPredicateTriplesSum.collect.head(0).toString.toInt / slotSize
      var requiredSlots = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x)._2 / slotSize
      var requiredRepetition = 1
      if (requiredSlots > slots) requiredRepetition = requiredSlots / slots

      val commonPredicates = numberOfCommonTriples(lengthOfNumberOfCommonTriples - x)._1.toString
      println("In bigger loop: processing triples with number of commonPredicates equal to " + commonPredicates)

      var cluster = profile {
        clusters.where(clusters("commonPredicateCount") === commonPredicates)
      }

      //var matched = this.matchACluster(requiredRepetition, typeSubjectWithLiteral, cluster, matchedEmptyDF)
      var matched = profile {
        this.matchAClusterOptimized(requiredRepetition, typeSubjectWithLiteral, cluster, matchedEmptyDF)
      }

      if (x == 0) {
        matchedUnion = matched
      }
      else {
        println("doing union")
        matchedUnion = profile {
          matchedUnion.union(matched)
        }
      }
    }
    matchedUnion
  }


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
        println("firstMatchingLevel: ")
        firstMatchingLevel.show(40, 80)
      }
      var matched = getMatchedEntitiesBasedOnLiteralSim(firstMatchingLevel)
      if (printReport) {
        println("matched: ")
        matched.show(40, 80)
      }

      localUnion.union(matched)
      println("In parallel cluster number " + counter + " from " + requiredRepetition)
      counter = counter + 1
    })
    localUnion
  }


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

  def getMatchedEntitiesBasedOnLiteralSim(firstMatchingLevel: DataFrame): DataFrame = {
    val simHandler = new SimilarityHandler(0.7)
    val similarPairs = firstMatchingLevel.collect().map(x => (x.getString(0), x.getString(2),
      simHandler.areLiteralsEqual(x.getString(1), x.getString(3))))

    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val matched = rdd1.toDF("Subject1", "Subject2", "equal")
    if (printReport) {
      matched.show(40)
    }
    matched.createOrReplaceTempView("matched")
    val sqlText1 = "SELECT Subject1, Subject2 FROM matched where equal = true"
    val subjects = sparkSession.sql(sqlText1)
    if (printReport) {
      subjects.show(50, 80)
    }
    println("the number of matched pair of subjects that are paired by matched predicate and matched literals")
    val sqlText3 = "SELECT count(*) FROM matched where equal = true"
    val typedTriples3 = sparkSession.sql(sqlText3)
    typedTriples3.show()

    subjects
  }

  //def getLevel2MatchedEntities(secondMatchingLevel: DataFrame, firstMatchedLevelEntities: DataFrame): DataFrame = {

//1.    wordnet comparison on entities, comparison is URI based, and we ignore literals
    // 1. wordnet comparison on entities, comparison is URI based, and we ignore literals
    // 2. in the column based literal process we use string based similarity , we use that similarity (can be sum of probabilites ). we calculate a number for the match
    // 3.add matched subject1 to a column to subject2 as subjectMatched (instead of fetching each triple from the matched dataset)
    // and check if a,b,c and a2,b,c exist
    //then those entities that have a matched relation in the comparison must have added similarity
 // }


  //rank the predicate and literals that are repeated the most. and give them less similarity point


}