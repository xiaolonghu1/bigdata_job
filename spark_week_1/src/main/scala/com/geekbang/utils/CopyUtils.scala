package com.geekbang.utils

import java.io.FileNotFoundException
import java.net.URI

import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory._

import scala.util.{Failure, Success, Try}

object CopyUtils {

  val log: Logger = getLogger("=============")

  def handleCopy(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, taskAttemptID: Long): String = {

    val r = {
      if (definition.source.isDirectory) {
        log.info("definition is directory " + definition)
        CopyUtils.createDirectory(destFS, definition)
      }
      else if (definition.source.isFile) {
        log.info("definition is file " + definition)
        CopyUtils.copyFile(sourceFS, destFS, definition, taskAttemptID)
      }
      else
        throw new UnsupportedOperationException(s"Given file is neither file nor directory. Copy unsupported: ${definition.source.getPath}")
    }

    log.info("handle copy " + r)
    r
  }


  private[utils] def createDirectory(destFS: FileSystem, definition: SingleCopyDefinition): String = {
    val destPath = new Path(definition.destination)
    if (destFS.exists(destPath)) {
      var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,destination is exist"
      log.info(result)
      result
    }
    else {
      val result = Try {
        if (destFS.exists(destPath.getParent)) {
          destFS.mkdirs(destPath)
          var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,make parent dir"
          result
        }
        else throw new FileNotFoundException(s"Parent folder [${destPath.getParent}] does not exist.")
      }.recover {
        case _: FileAlreadyExistsException =>
          var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,file is exist"
          result

      }
      result match {
        case Success(v) => v
        case Failure(e) =>
          log.error(s"Exception whilst creating directory [${definition.destination}]", e.toString)
          "error"
      }
    }
  }


  private[utils] def copyFile(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, taskAttemptID: Long): String = {
    val destPath = new Path(definition.destination)
    log.info("start to copy files")
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) =>
        val filecopy = performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = false, taskAttemptID)
        log.info("fail to copy file " + filecopy)
        filecopy
      case Failure(e) =>
        log.error("fail to  get destination file " + definition.destination, e.toString)
        ""
      case Success(_) =>
        performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = true, taskAttemptID)
        val filecopy: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,success to copy file"
        filecopy

    }
  }


  def performCopy(sourceFS: FileSystem, sourceFile: SerializableFileStatus, destFS: FileSystem, dest: URI, removeExisting: Boolean, taskAttemptID: Long): String = {

    val destPath = new Path(dest)

    val tempPath = new Path(destPath.getParent, s".sparkdistcp.$taskAttemptID.${destPath.getName}")

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourceFile.getPath))
        if (!destFS.exists(tempPath.getParent)) throw new RuntimeException(s"Destination folder [${tempPath.getParent}] does not exist")
        out = Some(destFS.create(tempPath, false))
        IOUtils.copyBytes(in.get, out.get, sourceFS.getConf.getInt("io.file.buffer.size", 4096))

      } catch {
        case e: Throwable => throw e
      } finally {
        in.foreach(_.close())
        out.foreach(_.close())
      }
    }.map {
      _ =>
        val tempFile = destFS.getFileStatus(tempPath)
        if (sourceFile.getLen != tempFile.getLen)
          throw new RuntimeException(s"Written file [${tempFile.getPath}] length [${tempFile.getLen}] did not match source file [${sourceFile.getPath}] length [${sourceFile.getLen}]")

        if (removeExisting) {
          val res = destFS.delete(destPath, false)
          if (!res) throw new RuntimeException(s"Failed to clean up existing file [$destPath]")
        }
        if (destFS.exists(destPath)) throw new RuntimeException(s"Cannot create file [$destPath] as it already exists")
        val res = destFS.rename(tempPath, destPath)
        if (!res) throw new RuntimeException(s"Failed to rename temporary file [$tempPath] to [$destPath]")
    } match {
      case Success(_) if removeExisting =>
        val result: String = s"Source: [${sourceFile.getPath.toUri}], Destination: [${dest}] ,perform copy,remove existing file"
        result
      case Success(_) =>
        val result: String = s"Source: [${sourceFile.getPath.toUri}], Destination: [${dest}] ,perform copy"
        result
      case Failure(e) =>
        log.error(s"Failed to copy file [${sourceFile.getPath}] to [$destPath]", e.toString)
        "error"
    }

  }


}
