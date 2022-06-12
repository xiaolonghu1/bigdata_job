package com.geekbang.utils

import java.net.URI
import java.util.concurrent.Executors

import com.owen.bigdata.SparkDistCP.KeyedCopyDefinition
import com.liveramp.objects.CopyDefinitionWithDependencies
import com.owen.bigdata.objects.{CopyDefinitionWithDependencies, SerializableFileStatus, SingleCopyDefinition}
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object HandleFileUtils {

  val log: Logger = getLogger("=============")

  private implicit class ScalaRemoteIterator[T](underlying: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = underlying.hasNext

    override def next(): T = underlying.next()
  }

  def getFileList(fs: FileSystem, path: Path): Seq[(SerializableFileStatus, Seq[SerializableFileStatus])] = {


    //文件列表类
    var fileLister = new FileLister(fs, path)
    val exceptions = new java.util.concurrent.ConcurrentLinkedQueue[Exception]()

    val pool = Executors.newFixedThreadPool(ParameterUtils.getThreadWhileListFiles())

    log.info( s"Scanning  [$path] for file list")
    val tasks: Seq[Future[Unit]] = List.fill(1)(fileLister).map(pool.submit).map(j => Future {
      j.get()
      ()
    }(scala.concurrent.ExecutionContext.global))

    import scala.concurrent.ExecutionContext.Implicits.global

    Await.result(Future.sequence(tasks), Duration.Inf)
    pool.shutdown()
    val toProcess = fileLister.getToProcess()
    if (!toProcess.isEmpty) log.error( "Error during file listing", "Exception listing files, toProcess queue was not empty")


    if (!exceptions.isEmpty) {
      val collectedExceptions = exceptions.iterator().asScala.toList
      collectedExceptions
        .foreach {
          e => log.error( "Exception during file listing", e.toString)
        }
      throw collectedExceptions.head
    }

    log.info( s"Finished recursive list of [$path]")

    //java scala集合互相转换
    var filelist = fileLister.getProcessed().iterator().asScala.toSeq
    log.info( "file list: " + filelist)
    filelist
  }


  def getFilesFromSourceHadoop(sparkContext: SparkContext, sourceURI: URI, destinationURI: URI, numTasks: Int): RDD[KeyedCopyDefinition] = {
    // parallelize并行化集合是根据一个已经存在的Scala集合创建的RDD对象。集合的里面的元素将会被拷贝进入新创建出的一个可被并行操作的分布式数据集。
    // 如果不指定numSlices参数，将会根据系统环境来进行切分多个partition（slice），每一个partition（slice）启动一个Task来进行处理
    // 如果指定numSlices参数，将会创建指定个数的partition（slice）
    log.info( "number of tasks is: " + numTasks)
    val sourceFS = new Path(sourceURI).getFileSystem(sparkContext.hadoopConfiguration)
    var sourceRDD = sparkContext
      .parallelize(HandleFileUtils.getFileList(sourceFS, new Path(sourceURI)), numTasks)
      .map {
        case (f, d) =>

          val dependentFolders = d.map {
            dl =>
              val udl = PathUtils.sourceURIToDestinationURI(dl.uri, sourceURI, destinationURI)
              SingleCopyDefinition(dl, udl)
          }
          val fu = PathUtils.sourceURIToDestinationURI(f.uri, sourceURI, destinationURI)
          CopyDefinitionWithDependencies(f, fu, dependentFolders)
      }.map(_.toKeyedDefinition)

    sourceRDD
  }

  def getFilesFromDestinationHadoop(sparkContext: SparkContext, destinationPath: Path): RDD[(URI, SerializableFileStatus)] = {
    val destinationFS = destinationPath.getFileSystem(sparkContext.hadoopConfiguration)
    sparkContext
      .parallelize(HandleFileUtils.getFileList(destinationFS, destinationPath))
      .map { case (f, _) => (f.getPath.toUri, f) }
  }

}
