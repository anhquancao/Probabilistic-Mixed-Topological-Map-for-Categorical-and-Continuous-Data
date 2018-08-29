package com.quan.util

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import breeze.linalg.Vector
import com.quan.model.Cell

object IOHelper {
  def write(fileName: String, content: String, append: Boolean = true) = {

    val pw = new FileWriter(fileName, append)
    pw.write(content)
    pw.close()
  }

  def writeLabels(iter: Int, dataWithLabels: Array[(Long, (Vector[Double], Int))], dirName: String): Unit = {

    val labelsFileName = dirName + "/labels-" + iter

    Files.createFile(Paths.get(labelsFileName))

    for (index <- dataWithLabels.indices) {
      val data = dataWithLabels(index)._2._1.toArray
      for (v <- data.indices) {
        write(labelsFileName, data(v) + ",")
      }
      write(labelsFileName, dataWithLabels(index)._2._2 + "")
      write(labelsFileName, "\n")
    }
  }

  def writeIndices(fileName: String, value: String): Unit = {
    write(fileName, value)
    write(fileName, "\n")

  }

  def writeCells(iter: Int, numRows: Int, numCols: Int, cells: Array[Array[Cell]], dirName: String): Unit = {

    val probFilename = dirName + "/prob-" + iter
    val itemsFilename = dirName + "/items-" + iter
    val epsilonFileName = dirName + "/epsilon" + iter
    val contCentroidFileName = dirName + "/contCentroid" + iter
    val binCentroidFileName = dirName + "/binCentroid" + iter

    Files.createFile(Paths.get(probFilename))
    Files.createFile(Paths.get(itemsFilename))
    Files.createFile(Paths.get(epsilonFileName))
    Files.createFile(Paths.get(contCentroidFileName))
    Files.createFile(Paths.get(binCentroidFileName))


    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        val cell = cells(row)(col)
        write(probFilename, cell.prob + "")
        write(itemsFilename, cell.numItems + "")
        write(epsilonFileName, cell.binEpsilon + "")
        write(contCentroidFileName, cell.contMean.toArray.mkString(",") + "\n")
        write(binCentroidFileName, cell.binMean.toArray.mkString(",") + "\n")
        if (col != numCols - 1) {
          write(probFilename, ",")
          write(itemsFilename, ",")
          write(epsilonFileName, ",")
        }
      }
      write(probFilename, "\n")
      write(itemsFilename, "\n")
      write(epsilonFileName, "\n")
    }
  }
}
