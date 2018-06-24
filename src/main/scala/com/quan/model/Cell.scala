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
  val prob: Double = 1.0 / (AppContext.gridSize._1 * AppContext.gridSize._2)
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

//  println("contStd: " + contStd)
//  println("binEpsilon: " + binEpsilon)

  //    println("B Mean: " + binMean.size)
  //    println(binMean)
  //    println("R Mean: " + contMean.size)
  //    println(contMean)
  //    println("BEpsilon: " + binEpsilon)

  def distanceTo(other: Cell): Int = {
    val loc1 = (this.row, this.col)
    val loc2 = (other.row, other.col)
    DistributionHelper.kernel(loc1, loc2)
  }

//  def computeDistribution(iter: Int) = {
//    val T: Double = AppContext.TMax * scala.math.pow(AppContext.TMin / AppContext.TMax, iter / AppContext.maxIter)
//    val contData: RDD[(Long, Vector[Double])] = AppContext.getContData
//    val binData: RDD[(Long, Vector[Int])] = AppContext.getBinData
//
//    val binBernouli: RDD[(Long, Double)] = binData.mapValues(v => {
//      DistributionHelper.bernouli(v, binMean, binEpsilon)
//    })
//
//    val contGaussian: RDD[(Long, Double)] = contData.mapValues(v => {
//      DistributionHelper.gaussian(v, contMean, contStd)
//    })
//
////    pXContOverC = contGaussian.map(_._2).reduce(_ + _) // p(x cont/c)
////    pXBinOverC = binBernouli.map(_._2).reduce(_ + _) // p(x binary/c)
////    pXOverC = pXBinOverC * pXContOverC
////    println("p(x cont/c)= " + pXContOverC)
////    println("p(x binary/c)= " + pXBinOverC)
////    println("p(x/c)=" + pXOverC)
//  }

}
