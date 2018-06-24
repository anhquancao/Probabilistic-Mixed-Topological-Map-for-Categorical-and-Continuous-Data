package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}

/**
  *
  * @param rowI
  * @param colI
  */
class Cell(val rowI: Int, val colI: Int) extends Serializable {
  val col: Int = colI
  var prob: Double = 1.0 / (AppContext.gridSize._1 * AppContext.gridSize._2)
  var row: Int = rowI
  var pXContOverC: Double = 0
  var pXBinOverC: Double = 0
  var pXOverC: Double = 0


  // continuous parameters
  var contStd: Double = AppContext.getRandom.nextDouble
  var contMean: Vector[Double] = RandomHelper.createRandomDoubleVector(AppContext.contSize)

  // binary parameters
  var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(AppContext.binSize)
  var binEpsilon: Double = AppContext.getRandom.nextDouble() / 2

}
