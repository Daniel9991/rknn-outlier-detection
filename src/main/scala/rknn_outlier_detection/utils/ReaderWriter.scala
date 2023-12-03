package rknn_outlier_detection.utils

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ArrayBuffer

object ReaderWriter {

  def readCSV(
    filename: String,
    separator: String = ",",
    hasHeader: Boolean = true
  ): Array[Array[String]] = {

    val data = new ArrayBuffer[Array[String]]

    val bufferedSource = scala.io.Source.fromFile(filename)

    for (line <- bufferedSource.getLines) {
      val cols = line.split(separator).map(_.trim)
      data += cols
    }

    bufferedSource.close

    if (hasHeader) data.toArray.slice(1, data.length) else data.toArray
  }

  def writeToFile(
    filename: String,
    data: String,
 ): Unit = {

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write(data)

    bw.close()
  }
}
