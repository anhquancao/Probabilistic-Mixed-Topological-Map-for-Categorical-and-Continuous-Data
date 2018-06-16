package com.quan.context.util

import com.quan.context.AppContext

object RandomHelper {
  def createRandomDoubleVector(size: Int): Vector[Double] = {
    val temp = for (i <- 0 until size)
      yield AppContext.getRandom.nextDouble()
    temp.toVector
  }

  def createRandomBinaryVector(size: Int): Vector[Int] = {
    val temp = for (i <- 0 until size)
      yield AppContext.getRandom.nextInt(2)
    temp.toVector
  }
}
