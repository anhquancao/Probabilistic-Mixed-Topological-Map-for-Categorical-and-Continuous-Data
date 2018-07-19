package com.quan.model

import breeze.linalg._
import com.quan.util.DistributionHelper
import org.apache.spark.rdd.RDD

class BinaryModel(val numRows: Int, val numCols: Int) extends Serializable {
  def logPXOverC(binData: RDD[(Long, Vector[Int])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    println("Bin Model: pXOverC")
    binData.mapValues(x => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield DistributionHelper.logBernouli(x, cells(row)(col).binMean, cells(row)(col).binEpsilon)
          ).toArray
      temp.toArray
    })
  }

  def mean(pCOverX: RDD[(Long, Array[Array[Double]])], binData: RDD[(Long, Vector[Int])]): Array[Array[DenseVector[Int]]] = {
    println("Bin Model: mean")
    val leftAndRightParts = pCOverX.join(binData).map(v => {
      val x: Vector[Double] = new DenseVector[Double](v._2._2.toArray.map(_.toDouble))
      val pC: Array[Array[Double]] = v._2._1
      (pC.map(row => row.map(value => (1.0 - x) * value)), pC.map(row => row.map(value => x * value)))
    }).reduce((v1, v2) => {
      val leftPart: Array[Array[Vector[Double]]] = v1._1.zip(v2._1).map {
        case (x, y) => x.zip(y)
          .map { case (x1, y1) => x1 + y1 }
      }
      val rightPart: Array[Array[Vector[Double]]] = v1._2.zip(v2._2).map {
        case (x, y) => x.zip(y)
          .map { case (x1, y1) => x1 + y1 }
      }
      (leftPart, rightPart)
    })

    val left = leftAndRightParts._1
    val right = leftAndRightParts._2

    left.zip(right).map {
      case (rLeft, rRight) => {
        rLeft.zip(rRight).map { case (lVec, rVec) => {
          val lArr = lVec.toArray
          val rArr = rVec.toArray
          val arr = lArr.zip(rArr).map { case (lVal, rVal) => {
            if (lVal > rVal) 0 else 1
          }
          }
          new DenseVector[Int](arr)
        }
        }
      }
    }
  }

  def std(pCOverX: RDD[(Long, Array[Array[Double]])],
          binMean: Array[Array[DenseVector[Int]]],
          binData: RDD[(Long, Vector[Int])],
          binSize: Int
         ): Array[Array[Double]] = {
    println("Bin Model: std")
    val numerator: Array[Array[Double]] = pCOverX.join(binData).map { case (i, data: (Array[Array[Double]], Vector[Int])) => {
      val pC: Array[Array[Double]] = data._1
      val x: Vector[Int] = data._2
      val hamDist: Array[Array[Double]] = binMean.map(_.map(DistributionHelper.hammingDistance(x, _)))
      hamDist.zip(pC).map { case (hamDistRow: Array[Double], pCRow: Array[Double]) => {
        hamDistRow.zip(pCRow).map { case (dist: Double, p: Double) => dist * p }
      }
      }
    }
    }.reduce((v1, v2) => {
      v1.zip(v2).map {
        case (x, y) => {
          x.zip(y).map { case (x1, y1) => x1 + y1 }
        }
      }
    })

    val denumerator: Array[Array[Double]] = pCOverX.map {
      case (index: Long, data: Array[Array[Double]]) => {
        data.map(_.map(_ * binSize))
      }
    }.reduce {
      case (v1: Array[Array[Double]], v2: Array[Array[Double]]) => {
        v1.zip(v2).map {
          case (rV1: Array[Double], rV2: Array[Double]) => {
            rV1.zip(rV2).map {
              case (x1: Double, x2: Double) => x1 + x2
            }
          }
        }
      }
    }

    numerator.zip(denumerator).map {
      case (num: Array[Double], denum: Array[Double]) => {
        num.zip(denum).map {
          case (numVal: Double, denumVal: Double) => numVal / denumVal
        }
      }
    }
  }

}
