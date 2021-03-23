import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import java.net.URI

object HdfsService {
  private val configuration = new Configuration()
  private val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration)

  private val partitionFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.startsWith("date=")
  }
  private val partFileFilter = new PathFilter {
    override def accept(path: Path): Boolean = fileSystem.getFileStatus(path).isFile &&
      path.getName.startsWith("part-")
  }

  def compactDirectory(source: String, destination: String): Unit = {
    fileSystem.listStatus(new Path(source), partitionFilter)
      .map(_.getPath)
      .foreach(partitionPath => {
        val destinationDirectory = createFolder(destination, partitionPath.getName)
        copyAndMerge(partitionPath, new Path(destinationDirectory, "part-0000"))
      })
  }

  private def writeFile(outputFile: FSDataOutputStream, status: FileStatus): Unit = {
    val inputFile = fileSystem.open(status.getPath)
    IOUtils.copyBytes(inputFile, outputFile, configuration, false)
    inputFile.close()
  }

  private def writeFile(outputFile: FSDataOutputStream, status: FileStatus, suffix: Char): Unit = {
    writeFile(outputFile, status)
    outputFile.write(suffix)
  }

  private def copyAndMerge(sourceDirectory: Path, destinationFile: Path): Unit = {
    val outputFile = fileSystem.create(destinationFile)
    val statuses = fileSystem
      .listStatus(sourceDirectory, partFileFilter)
      .filter(_.getLen > 0)
      .sortBy(_.getPath.getName)

    statuses
      .zipWithIndex
      .collect(tuple => {
        if(tuple._2 < statuses.length - 1) {
          writeFile(outputFile, tuple._1, '\n')
        } else {
          writeFile(outputFile, tuple._1)
        }
      })
    outputFile.close()
  }

  private def createFolder(root: String, folderPath: String): Path = {
    val path = new Path(root, folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
    path
  }
}
