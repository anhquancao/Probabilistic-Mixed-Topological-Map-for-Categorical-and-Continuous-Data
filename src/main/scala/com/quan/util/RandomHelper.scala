package com.quan.util

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.model.Cell

object RandomHelper {
  def createRandomDoubleVector(size: Long): Vector[Double] = {
    val temp = for (i <- 0 until size.toInt)
      yield AppContext.getRandom.nextDouble()
    new DenseVector[Double](temp.toArray)
  }

  def createRandomBinaryVector(size: Int): Vector[Int] = {
    val temp = for (i <- 0 until size)
      yield AppContext.getRandom.nextInt(2)
    new DenseVector[Int](temp.toArray)
  }

  def create2dArray(numRows: Int, numCols: Int, value: Double): Array[Array[Double]] = {
    val arr: Array[Array[Double]] = (for (row <- 0 until numRows)
      yield (for (col <- 0 until numCols) yield value).toArray
      ).toArray
    arr
  }

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

  def writeCells(iter: Int, numRows: Int, numCols: Int, cells: Array[Array[Cell]], dirName: String): Unit = {

    val probFilename = dirName + "/prob-" + iter
    val itemsFilename = dirName + "/items-" + iter

    Files.createFile(Paths.get(probFilename))
    Files.createFile(Paths.get(itemsFilename))

    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        val cell = cells(row)(col)
        write(probFilename, cell.prob + "")
        write(itemsFilename, cell.numItems + "")
        if (col != numCols - 1) {
          write(probFilename, ",")
          write(itemsFilename, ",")
        }
      }
      write(probFilename, "\n")
      write(itemsFilename, "\n")
    }
  }
}
