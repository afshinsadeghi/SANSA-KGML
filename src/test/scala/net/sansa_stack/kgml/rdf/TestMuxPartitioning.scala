package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

import scala.reflect.ClassTag

object TestMuxPartitioning {


  def main(args: Array[String]): Unit = {

// .toDF("ID", "NAME", "SEQ", "NUMBER")
    val valueTuple = Seq(
      ("A", "John", 1, 3),
      ("A", "Bob", 2, 5),
      ("A", "Sam", 3, 1),
      ("B", "Kim", 1, 4),
      ("B", "John", 2, 3),
      ("B", "Ria", 3, 5))

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SparkMuxParTest")
      .getOrCreate()
    val rdd = spark.sqlContext.sparkContext.parallelize(valueTuple)

    val result = this.splitSampleMux(rdd, 3, MEMORY_ONLY, 0)

    println("the input: ")
    rdd.foreach(println)
    println("result: ")
    result.foreach(a => a.collect().foreach(println))

  }
  def muxPartitions[T :ClassTag](rdd: RDD[T],n: Int, f: (Int, Iterator[T]) => Seq[T],
                                 persist: StorageLevel): Seq[RDD[T]] = {
    val mux = rdd.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => Iterator.single(itr.next()(j)) } }
  }

  def flatMuxPartitions[T :ClassTag](rdd: RDD[T], n: Int, f: (Int, Iterator[T]) => Seq[TraversableOnce[T]],
                                     persist: StorageLevel): Seq[RDD[T]] = {
    val mux = rdd.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => itr.next()(j).toIterator } }
  }

  import org.apache.spark.storage.StorageLevel._
  def splitSampleMux[T :ClassTag](rdd: RDD[T], n: Int,
                                  persist: StorageLevel = MEMORY_ONLY,
                                  seed: Long = 42): Seq[RDD[T]] =
    this.flatMuxPartitions(rdd, n, (id: Int, data: Iterator[T]) => {
      scala.util.Random.setSeed(id.toLong * seed)
      val samples = Vector.fill(n) { scala.collection.mutable.ArrayBuffer.empty[T] }
      data.foreach { e => samples(scala.util.Random.nextInt(n)) += e }
      samples
    }, persist)
}
