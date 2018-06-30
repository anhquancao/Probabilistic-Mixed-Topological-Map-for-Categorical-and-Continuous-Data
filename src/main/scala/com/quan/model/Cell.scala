package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}

/**
  *
  * @param rowI
  * @param colI
  */
class Cell(val rowI: Int, val colI: Int, val contSize: Int, val binSize: Int, var prob: Double) extends Serializable {
  val col: Int = colI
  var row: Int = rowI
  var pXContOverC: Double = 0
  var pXBinOverC: Double = 0
  var pXOverC: Double = 0


  // continuous parameters
  var contStd: Double = AppContext.getRandom.nextDouble
  var contMean: Vector[Double] = RandomHelper.createRandomDoubleVector(contSize)

  // binary parameters
  var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(binSize)
  var binEpsilon: Double = AppContext.getRandom.nextDouble() / 2

}
