package com.geekbang

import com.geekbang.objects.{ConfigSerializableDeser, CopyDefinitionWithDependencies, FileSystemObjectCacher}
import com.geekbang.utils.{CopyUtils, HandleFileUtils, PathUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Homework_2 {

  val log = LoggerFactory.getLogger("com.geekbang.Homework_2")
  var numTasks = 2
  var src = ""
  var dest = ""
  var ignoreErr = false
  var threadWhileListFiles = 1


  def main(args: Array[String]): Unit = {
    log.info("Start Distcp Job")
    parseArgs(args.toList)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ReplaceColumnValue Local Test")
      .set("spark.debug.maxToStringFields", "1100")
    val sparkSession = SparkSession.builder().getOrCreate()
    val source= new Path(src)
    val destination = new Path(dest)
  }


  def run(sparkSession: SparkSession, sourcePath: Path, destinationPath: Path): Unit = {
    val qualifiedSourcePath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, sourcePath)
    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, destinationPath)
    val sourceRDD = HandleFileUtils.getFilesFromSourceHadoop(sparkSession.sparkContext, qualifiedSourcePath.toUri, qualifiedDestinationPath.toUri,ParameterUtils.getNumTasks())
    log.info("source rdd "+sourceRDD.map(x => x.toString).reduce((x, y) => x + "," + y))
    val destinationRDD = HandleFileUtils.getFilesFromDestinationHadoop(sparkSession.sparkContext, qualifiedDestinationPath)
    log.info("destination rdd "+destinationRDD.map(x => x.toString).reduce((x,y) => x + "," + y))
    log.info("SparkDistCP tasks number : \n" + sourceRDD.partitions.length)
    val joined = sourceRDD.fullOuterJoin(destinationRDD)
    log.info("joined rdd "+joined.map(x => x.toString).reduce((x,y) => x + "," + y))
    val toCopy = joined.collect { case (_, (Some(s), _)) => s }


    val copyResult: RDD[String] = doCopy(toCopy)

    copyResult.foreach(_ => ())
    log.info("Success to copy files")



  }


  private def doCopy(sourceRDD: RDD[CopyDefinitionWithDependencies]): RDD[String] = {


    val serConfig = new ConfigSerializableDeser(sourceRDD.sparkContext.hadoopConfiguration)

    log.info("do copy START ")
    var distcpresult = sourceRDD
      .mapPartitions {
        iterator =>
          val hadoopConfiguration = serConfig.get()
          val attemptID = TaskContext.get().taskAttemptId()
          val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

          iterator
            .flatMap(_.getAllCopyDefinitions)
            .collectMapWithEmptyCollection((d, z) => z.contains(d),
              d => {
                val r = CopyUtils.handleCopy(fsCache.getOrCreate(d.source.uri), fsCache.getOrCreate(d.destination), d, attemptID)
                r
              }
            )
      }
    log.info("do copy END ")
    distcpresult
  }

  private implicit class DistCPIteratorImplicit[B](iterator: Iterator[B]) {

    def collectMapWithEmptyCollection(skip: (B, Set[B]) => Boolean, action: B => String): Iterator[String] = {

      iterator.scanLeft((Set.empty[B], None: Option[String])) {
        case ((z, _), d) if skip(d, z) => (z, None)
        case ((z, _), d) =>
          (z + d, Some(action(d)))
      }
        .collect { case (_, Some(r)) => r }

    }

  }

  def parseArgs(args: List[String]): Unit = args match {
    case "--numTasks" :: value :: tail =>
      numTasks = value.toInt
      parseArgs(tail)
    case "--source" :: value :: tail =>
      src  = value
      parseArgs(tail)
    case "--dest" :: value :: tail =>
      dest  = value
      parseArgs(tail)
    case "--ignoreErrors" :: value :: tail =>
      ignoreErr  = value.toBoolean
      parseArgs(tail)
    case "--thread" :: value :: tail =>
      threadWhileListFiles  = value.toInt
      parseArgs(tail)
    case Nil =>
    case _ :: _ :: tail => parseArgs(tail)
    case _ =>
  }
}
