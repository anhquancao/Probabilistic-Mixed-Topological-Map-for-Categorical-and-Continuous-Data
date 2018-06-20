package com.quan.model

import com.quan.util.DistributionHelper

class Cell(val rowI: Int, val colI: Int) {
  val col: Int = colI
  val prob: Double = 0.0
  var row: Int = rowI

  def distanceTo(other: Cell): Int = {
    val loc1 = (this.row, this.col)
    val loc2 = (other.row, other.col)
    DistributionHelper.kernel(loc1, loc2)
  }
}
