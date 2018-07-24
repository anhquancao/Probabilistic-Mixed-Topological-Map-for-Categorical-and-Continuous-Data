package com.quan.model

import breeze.linalg._
import com.quan.util.{DistributionHelper, RandomHelper}
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

  def mean(pCOverX: RDD[(Long, Array[Double])], binData: RDD[(Long, Vector[Int])]): Array[Array[DenseVector[Int]]] = {
    println("Bin Model: mean")
    val leftAndRightParts: (Array[Vector[Double]], Array[Vector[Double]]) = pCOverX.join(binData).map(v => {
      val x: Vector[Double] = new DenseVector[Double](v._2._2.toArray.map(_.toDouble))
      val pC: Array[Double] = v._2._1
      (pC.map(value => (1.0 - x) * value), pC.map(value => x * value))
    }).reduce((v1, v2) => {
      val leftPart: Array[Vector[Double]] = v1._1.zip(v2._1).map { case (x, y) => x + y }

      val rightPart: Array[Vector[Double]] = v1._2.zip(v2._2).map { case (x, y) => x + y }
      (leftPart, rightPart)
    })

    val left: Array[Vector[Double]] = leftAndRightParts._1
    val right: Array[Vector[Double]] = leftAndRightParts._2

    val res: Array[DenseVector[Int]] = left.zip(right).map {
      case (lVec, rVec) => {
        val lArr = lVec.toArray
        val rArr = rVec.toArray
        val arr = lArr.zip(rArr).map {
          case (lVal, rVal) => {
            if (lVal > rVal) 0 else 1
          }
        }
        new DenseVector[Int](arr)
      }
    }
    (
      for (row <- 0 until numRows) yield (
        for (col <- 0 until numCols)
          yield res(row * numCols + col)
        ).toArray
      ).toArray
  }

  def epsilon(pCOverX: RDD[(Long, Array[Double])],
              binMean: Array[Array[DenseVector[Int]]],
              binData: RDD[(Long, Vector[Int])],
              binSize: Int
             ): Array[Array[Double]] = {
    println("Bin Model: epsilon")
    val numerator: Array[Array[Double]] = pCOverX.join(binData).map {
      case (i, data: (Array[Double], Vector[Int])) => {
        val pC: Array[Double] = data._1
        val x: Vector[Int] = data._2
        val hamDist: Array[Array[Double]] = binMean.map(_.map(DistributionHelper.hammingDistance(x, _)))

        val res: Array[Array[Double]] = RandomHelper.create2dArray(numRows, numCols, 0.0)
        for (row <- 0 until numRows) {
          for (col <- 0 until numCols) {
            res(row)(col) = hamDist(row)(col) * pC(DistributionHelper.index(row, col, numCols))
          }
        }
        res
      }
    }.reduce((v1, v2) => {
      v1.zip(v2).map {
        case (x, y) => {
          x.zip(y).map { case (x1, y1) => x1 + y1 }
        }
      }
    })

    val denumerator: Array[Array[Double]] = pCOverX.map {
      case (index: Long, data: Array[Double]) => {
        val res: Array[Array[Double]] = RandomHelper.create2dArray(numRows, numCols, 0.0)
        for (row <- 0 until numRows) {
          for (col <- 0 until numCols) {
            res(row)(col) = data(DistributionHelper.index(row, col, numCols)) * binSize
          }
        }
        res
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
