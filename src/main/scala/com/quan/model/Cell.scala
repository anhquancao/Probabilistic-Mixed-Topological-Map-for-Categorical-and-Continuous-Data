package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}

/**
  *
  * @param rowI
  * @param colI
  */
class Cell(val rowI: Int, val colI: Int, val contSize: Int,
           val binSize: Int, var prob: Double,
           var contMean: Vector[Double],
           var binMean: Vector[Int],
           numRows: Int,
           numCols: Int) extends Serializable {
  val col: Int = colI
  var row: Int = rowI
  var numItems = 0.0

  // continuous parameters
  var contVariance: Double = 1.0 / (4 * numRows * numCols)
  //  var contMean: Vector[Double] = RandomHelper.createRandomDoubleVector(contSize)

  // binary parameters
  //  var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(binSize)
  var binEpsilon: Double = AppContext.getRandom.nextDouble() / 2

}
