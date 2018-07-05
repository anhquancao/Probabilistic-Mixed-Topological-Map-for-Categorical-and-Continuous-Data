package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}

/**
  *
  * @param rowI
  * @param colI
  */
class Cell(val rowI: Int, val colI: Int, val contSize: Int, val binSize: Int, var prob: Double,
           var contMean: Vector[Double],
           var binMean: Vector[Int]) extends Serializable {
  val col: Int = colI
  var row: Int = rowI

  // continuous parameters
  var contStd: Double = AppContext.getRandom.nextDouble
  //  var contMean: Vector[Double] = RandomHelper.createRandomDoubleVector(contSize)

  // binary parameters
  //  var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(binSize)
  var binStd: Double = AppContext.getRandom.nextDouble() / 2

}
