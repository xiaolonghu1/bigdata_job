package com.geekbang.utils

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.Logger

class HDFS(url: URI, conf: Configuration = new Configuration()) extends java.io.Serializable {

  val log = Logger.getLogger("utils.HDFS")
  val interval = 19000

  val hdfs = FileSystem.get(url, conf)

  def exists(path: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.exists $path $retry")
    try {
      val p = new Path(path)
      val status = hdfs.globStatus(p)
      var result = false
      if (status != null) {
        status.foreach { fileStatus =>
          if (!result) result = hdfs.exists(fileStatus.getPath())
        }
      }
      result
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        //Thread.sleep(interval)
        exists(path, retry - 1)
    }
  }

  def upload(src: String, dst: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.upload $src $dst $retry")
    try {
      val s = new Path(src)
      val d = new Path(dst)

      hdfs.copyFromLocalFile(s, d)
      true
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        upload(src, dst, retry - 1)
    }
  }

  def download(src: String, dst: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.download $src $dst $retry")
    try {
      val s = new Path(src)
      val d = new Path(dst)

      hdfs.copyToLocalFile(s, d)
      true
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        download(src, dst, retry - 1)
    }
  }

  def merge(src: String, dst: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.merge $src $dst $retry")
    try {
      var result = false
      val status = hdfs.globStatus(new Path(src))
      if (status != null) {
        val dstPath = new Path(dst)
        if (hdfs.exists(dstPath)) {
          hdfs.delete(dstPath)
        }
        val os = hdfs.create(dstPath)
        status.foreach { fileStatus =>
          val is = hdfs.open(fileStatus.getPath())
          val buffer = new Array[Byte](2048)
          var bytesRead = is.read(buffer)

          while (bytesRead > 0) {
            os.write(buffer, 0, bytesRead)
            bytesRead = is.read(buffer)
          }
          is.close()
        }
        os.close();
      }
      true
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        create(dst, retry - 1)
    }
  }

  def delete(dst: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.delete $dst $retry")
    try {
      val d = new Path(dst)
      val status = hdfs.globStatus(d)
      if (status == null) {
        return true
        hdfs.close()
      }
      val result = hdfs.delete(d)

      if (!result) delete(dst, retry - 1)
      result
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        delete(dst, retry - 1)
    }
  }

  def mkdir(dir: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.mkdir $dir $retry")
    try {
      val d = new Path(dir)

      val result = hdfs.mkdirs(d)

      if (!result) mkdir(dir, retry - 1)
      result
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        mkdir(dir, retry - 1)
    }
  }

  def create(dst: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.create $dst $retry")
    try {
      val d = new Path(dst)

      val result = hdfs.createNewFile(d)

      if (!result) create(dst, retry - 1)
      result
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        create(dst, retry - 1)
    }
  }

  def createAndWrite(dst: String, content: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.createAndWrite $dst $retry")
    try {
      val d = new Path(dst)

      val os = hdfs.create(d)
      os.write(content.getBytes("UTF-8"))
      os.close()
      true
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        create(dst, retry - 1)
    }
  }

  def ls(path: String, retry: Int = 1): String = {

    // update by steven in 20180607
    if (!path.contains("*")) {
      if (retry <= 0) return ""
      log.info(s"HDFS.ls $path $retry")
      try {
        val fs = hdfs.listStatus(new Path(path))
        val listPath = FileUtil.stat2Paths(fs)
        listPath.mkString(",")
      }
      catch {
        case ex: Exception =>
          log.error(ex.getMessage())
          //Thread.sleep(interval)
          ls(path, retry - 1)
      }
    }
    else {
      val correctPath = path.substring(0, path.lastIndexOf("/")) + "/"
      val fileMark = path.substring(path.lastIndexOf("/") + 1, path.length).replace("*", "")

      if (retry <= 0) return ""
      log.info(s"HDFS.ls $correctPath $retry")
      try {
        val fs = hdfs.listStatus(new Path(correctPath))
        val listPath = FileUtil.stat2Paths(fs)
        listPath.filter {
          i =>
            i.toString.contains(fileMark)
        }.mkString(",")
      }
      catch {
        case ex: Exception =>
          log.error(ex.getMessage())
          //Thread.sleep(interval)
          ls(correctPath, retry - 1)
      }
    }

  }

  // update by steven in 20180710
  def getTime(filePath: String, retry: Int = 1): String = {
    if (retry <= 0) return ""
    log.info(s"HDFS.ls $filePath $retry")
    try {
      val fs = hdfs.listStatus(new Path(filePath))
      fs(0).getModificationTime.toString
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        //Thread.sleep(interval)
        ls(filePath, retry - 1)
    }
  }

  def lsMore(path: String, retry: Int = 1): String = {

    // update by steven in 20180607
    if (!path.contains("*")) {
      if (path.endsWith("/")) {
        ""
      }
      else {
        val correctPath = path.substring(0, path.lastIndexOf("/")) + "/"
        if (retry <= 0) return ""
        log.info(s"HDFS.ls $path $retry")
        try {
          val fsCorrect = hdfs.listStatus(new Path(correctPath))
          val listPath = FileUtil.stat2Paths(fsCorrect)
          listPath.filter(i =>
            i.toString != "" && toString != path).mkString(",")
        }
        catch {
          case ex: Exception =>
            log.error(ex.getMessage())
            //Thread.sleep(interval)
            ls(path, retry - 1)
        }
      }
    }
    else {
      val correctPath = path.substring(0, path.lastIndexOf("/")) + "/"
      val fileMark = path.substring(path.lastIndexOf("/") + 1, path.length).replace("*", "")

      if (retry <= 0) return ""
      log.info(s"HDFS.ls $correctPath $retry")
      try {
        val fs = hdfs.listStatus(new Path(correctPath))
        val listPath = FileUtil.stat2Paths(fs)
        listPath.filter {
          i =>
            !i.toString.contains(fileMark)
        }.mkString(",")
      }
      catch {
        case ex: Exception =>
          log.error(ex.getMessage())
          //Thread.sleep(interval)
          ls(correctPath, retry - 1)
      }
    }
  }

  def rename_s3(from: String, to: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.rename_s3 $from $to $retry")
    try {
      val fs = hdfs.listStatus(new Path(from))
      val listPath = FileUtil.stat2Paths(fs)
      listPath.foreach { path =>
        val fileName = path.getName()
        hdfs.rename(path, new Path(to + fileName))
      }
      true
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        rename_s3(from, to, retry - 1)
    }
  }

  def rename(from: String, to: String, retry: Int = 1): Boolean = {
    if (retry <= 0) return false
    log.info(s"HDFS.rename $from $to $retry")
    try {
      val result = hdfs.rename(new Path(from), new Path(to))
      result
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        rename(from, to, retry - 1)
    }
  }

  def read(path: String, retry: Int = 1): String = {
    if (retry <= 0) return ""
    log.info(s"HDFS.read $path $retry")
    try {
      val hdfs_path = new Path(path)
      if (hdfs.exists(hdfs_path)) {
        val is = hdfs.open(hdfs_path)
        val stat = hdfs.getFileStatus(hdfs_path)
        val buffer = new Array[Byte](stat.getLen().toInt)
        is.readFully(0, buffer)
        is.close()
        new String(buffer, "UTF-8")
      }
      else {
        throw new Exception(s"the file ${path} is not found.");
      }
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        read(path, retry - 1)
    }
  }

  def readHeader(path: String, retry: Int = 0): Option[String] = {
    if (retry < 0) return None
    log.info(s"HDFS.read $path $retry")
    var br:BufferedReader=null
    try {
      val hdfs_path = new Path(path)
      if (hdfs.exists(hdfs_path)) {
        br = new BufferedReader(new InputStreamReader(hdfs.open(hdfs_path)))
        val headerLine=br.readLine
        if(StringUtils.isBlank(headerLine)){
          None
        }else{
          Some(headerLine)
        }
      } else {
        throw new Exception(s"the file ${path} is not found.");
      }
    }
    catch {
      case e: Exception =>
        log.error(e.getMessage())
        Thread.sleep(interval)
        readHeader(path, retry - 1)
    }finally {
      if(br!=null) br.close()
    }
  }

  // update by steven in 20181113
  def getFolderSize(filePath: String, retry: Int = 1): Long = {
    if (retry <= 0) return 0.toLong
    log.info(s"HDFS.ls $filePath $retry")
    try {
      val fs = hdfs.listStatus(new Path(filePath))
      var folderSize = 0.toLong
      fs.foreach { fileDesc =>
        folderSize = folderSize + fileDesc.getLen
      }
      folderSize
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        getFolderSize(filePath, retry - 1)
    }
  }

  // update by steven in 20181113
  def getFileSize(filePath: String, retry: Int = 1): Map[String, Long] = {
    var fileSize = Map.empty[String, Long]

    if (retry <= 0) return fileSize
    log.info(s"HDFS.ls $filePath $retry")
    try {
      val fs = hdfs.listStatus(new Path(filePath))
      var folderSize = 0.toLong
      fs.foreach { fileDesc =>
        fileSize += (fileDesc.getPath.toString -> fileDesc.getLen)
      }
      fileSize
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        getFileSize(filePath, retry - 1)
    }
  }

  def getFileTimeMap(filePath: String, retry: Int = 1): Map[String, Long] = {
    var fileTimeMap = Map.empty[String, Long]
    if (retry <= 0) return fileTimeMap
    log.info(s"HDFS.ls $filePath $retry")
    try {
      val fs = hdfs.listStatus(new Path(filePath))
      fs.foreach { fileDesc =>
        fileTimeMap += (fileDesc.getPath.toString -> fileDesc.getModificationTime)
      }
      fileTimeMap
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        getFileTimeMap(filePath, retry - 1)
    }
  }

  def getFileListSize(fileList:List[String], retry: Int = 1): Map[String, Long] = {
    var fileSize = Map.empty[String, Long]

    if (retry <= 0) return fileSize
    try {
      fileList.foreach{
        fileName =>
          val fs = hdfs.listStatus(new Path(fileName))
          fs.foreach{
            fileDesc =>
              fileSize += (fileDesc.getPath.toString -> fileDesc.getLen)
          }
      }
      fileSize
    }
    catch {
      case ex: Exception =>
        log.error(ex.getMessage())
        Thread.sleep(interval)
        getFileListSize(fileList, retry - 1)
    }
  }

}