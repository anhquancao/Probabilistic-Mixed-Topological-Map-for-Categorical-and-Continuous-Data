package com.quan.model

import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}

/**
  *
  * @param rowI
  * @param colI
  */
class Cell(val rowI: Int, val colI: Int) {
  val col: Int = colI
  val prob: Double = 0.0
  var row: Int = rowI

  // continuous parameters
  var RSignma = AppContext.getRandom.nextDouble
  var RStd = AppContext.getRandom.nextDouble
  var RMean = RandomHelper.createRandomDoubleVector(AppContext.contSize)

  // binary parameters
  var BMean = RandomHelper.createRandomBinaryVector(AppContext.binSize)
  var BEpsilon = AppContext.getRandom.nextDouble() / 2

  println("B Mean: " + BMean.size)
  println(BMean)
  println("R Mean: " + RMean.size)
  println(RMean)
  println("BEpsilon: " + BEpsilon)

  def distanceTo(other: Cell): Int = {
    val loc1 = (this.row, this.col)
    val loc2 = (other.row, other.col)
    DistributionHelper.kernel(loc1, loc2)
  }
}
