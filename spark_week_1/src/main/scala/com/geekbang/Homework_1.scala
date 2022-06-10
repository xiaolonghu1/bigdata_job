package com.geekbang

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scala.collection.mutable.Set

object Homework_1 {

  val log = LoggerFactory.getLogger("com.geekbang.Homework_1")

  def main(args: Array[String]): Unit = {
    log.info("Start Inverted Index Job")
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ReplaceColumnValue Local Test")
      .set("spark.debug.maxToStringFields", "1100")
    val sc = new SparkContext(conf)
    val input = List(("0", "it is what it is"), ("1", "what is it"), ("2", "it is a banana"))
    val rdd = sc.makeRDD(input).flatMap{ r =>
      val words = r._2.split(" ", -1)
      for (word <- words) yield (word, r._1)
    }.map( r => (r, 1))

    val index = rdd
      .reduceByKey((v1, _) => v1)
      .map(r => (r._1._1, Set(r._1._2)))
      .reduceByKey((v1, v2) => v1++v2)

    val indexRes = index.collect()
    for (i <- indexRes) {
      /**
       * 22/06/10 16:14:30 INFO Homework_1: (banana,Set(2))
       * 22/06/10 16:14:30 INFO Homework_1: (it,Set(0, 1, 2))
       * 22/06/10 16:14:30 INFO Homework_1: (is,Set(0, 1, 2))
       * 22/06/10 16:14:30 INFO Homework_1: (a,Set(2))
       * 22/06/10 16:14:30 INFO Homework_1: (what,Set(0, 1))
       */
      log.info("" + i)
    }

    val indexCount = rdd.reduceByKey((v1, v2) => v1 + v2)
      .map(r => (r._1._1, Set((r._1._2, r._2))))
      .reduceByKey((v1, v2) => v1++v2)

    val indexCountRes = indexCount.collect()
    for (i <- indexCountRes) {
      /**
       * 22/06/10 16:22:03 INFO Homework_1: (banana,Set((2,1)))
       * 22/06/10 16:22:03 INFO Homework_1: (it,Set((1,1), (0,2), (2,1)))
       * 22/06/10 16:22:03 INFO Homework_1: (is,Set((1,1), (0,2), (2,1)))
       * 22/06/10 16:22:03 INFO Homework_1: (a,Set((2,1)))
       * 22/06/10 16:22:03 INFO Homework_1: (what,Set((1,1), (0,1)))
       */
      log.info("" + i)
    }
  }
}
