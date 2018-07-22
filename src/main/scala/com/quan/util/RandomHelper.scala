package com.quan.util

import breeze.linalg._
import com.quan.context.AppContext

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
}
