package com.quan.util

import com.quan.context.AppContext
import com.quan.model.Cell
import breeze.linalg._
import org.apache.spark.rdd.RDD

object SOMHelper {
  /**
    *
    * @param numRows
    * @param numCols
    * @return
    */
  def createCells(numRows: Int, numCols: Int): Array[Array[Cell]] = {
    val temp = for (row <- 0 to numRows)
      yield (
        for (col <- 0 to numCols)
          yield new Cell(row, col)
        ).toArray
    temp.toArray
  }

  def computePXBinOverC(binData: RDD[(Long, Vector[Int])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    binData.mapValues(x => {
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield DistributionHelper.bernouli(x, cells(row)(col).binMean, cells(row)(col).binEpsilon)
          ).toArray
      temp.toArray
    })
  }

  def computePXContOverC(contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    contData.mapValues(x => {
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield DistributionHelper.gaussian(x, cells(row)(col).contMean, cells(row)(col).contStd)
          ).toArray
      temp.toArray
    })
  }

  def computePXOverC(binData: RDD[(Long, Vector[Int])], contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    // compute the bernouli of x over c
    val pXBinOverC: RDD[(Long, Array[Array[Double]])] = computePXBinOverC(binData, cells)

    // compute gaussian
    val pXContOverC: RDD[(Long, Array[Array[Double]])] = computePXContOverC(contData, cells)

    // compute the p(x/c)
    val pXOverC = pXBinOverC.join(pXContOverC).map(p => {
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield p._2._1(row)(col) * p._2._1(row)(col)
          ).toArray
      (p._1, temp.toArray)
    })

    pXOverC
  }

  // compute p(c/c*)
  def pCOverCStar(c: (Int, Int), cStar: (Int, Int), T: Double): Double = {
    var sum: Double = 0.0
    for (row <- 0 until AppContext.gridSize._1) {
      for (col <- 0 until AppContext.gridSize._2) {
        val r = (row, col)
        sum = sum + DistributionHelper.kernel(DistributionHelper.distance(cStar, r), T)
      }
    }
    DistributionHelper.kernel(DistributionHelper.distance(c, cStar), T) / sum
  }

  // compute p(x)
  def computePX(cells: Array[Array[Cell]], pXOverC: RDD[(Long, Array[Array[Double]])], T: Double): RDD[(Long, Double)] = {
    pXOverC.mapValues(v => {
      var pX: Double = 0.0
      for (rowStar <- 0 until AppContext.gridSize._1) {
        for (colStar <- 0 until AppContext.gridSize._2) {

          // get c*
          val cStar = (rowStar, colStar)

          // p(c*)
          val pCStar = cells(rowStar)(colStar).prob

          // p(x/c*)
          var pXOverCStar: Double = 0.0

          for (row <- 0 until AppContext.gridSize._1) {
            for (col <- 0 until AppContext.gridSize._2) {
              // get c
              val c = (row, col)

              // p(x/c)
              val pXOverCValue = v(row)(col)

              // p(c/c*)
              val pCOverCStar = SOMHelper.pCOverCStar(c, cStar, T)

              // p(x/c*) = sum p(x / c) * p(c/c*)
              pXOverCStar += pXOverCValue * pCOverCStar
            }
          }
          // p(x) = p(c*) x p(x/c*)
          pX += pCStar * pXOverCStar
        }
      }
      pX
    })

  }

  // compute p(c/x)
  def computePCOverX(pX: RDD[(Long, Double)],
                     pXOverC: RDD[(Long, Array[Array[Double]])],
                     cells: Array[Array[Cell]],
                     T: Double): RDD[(Long, Array[Array[Double]])] = {
    pX.join(pXOverC).map((v) => {
      val px: Double = v._2._1
      val pxOverC: Array[Array[Double]] = v._2._2

      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield 0.0
          ).toArray

      val pCOverXArr: Array[Array[Double]] = temp.toArray

      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {

          // get c*
          val c = (row, col)

          for (rowStar <- 0 until AppContext.gridSize._1) {
            for (colStar <- 0 until AppContext.gridSize._2) {

              // get c*
              val cStar = (rowStar, colStar)

              val pCStar: Double = cells(rowStar)(colStar).prob


              // p(c/c*)
              val pCOverCStar: Double = SOMHelper.pCOverCStar(c, cStar, T)

              // p(c, c*/ x)
              val pCAndCStar: Double = (pCOverCStar * pCStar * pxOverC(row)(col)) / px

              pCOverXArr(row)(col) += pCAndCStar

            }
          }

        }
      }
      (v._1, pCOverXArr)
    })
  }

  /**
    * p(c)
    *
    * @param pCOverX
    * @return
    */
  def computePC(pCOverX: RDD[(Long, Array[Array[Double]])]): Array[Array[Double]] = {
    val t = pCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })
    t.map(_.map(_ / AppContext.numberCells))
  }

  def computeT(iter: Int): Double = {
    AppContext.TMax *
      scala.math.pow(
        AppContext.TMin / AppContext.TMax,
        iter / AppContext.maxIter
      )
  }

  /**
    * compute the mean for continuous data w(c)
    *
    * @param pCOverX
    * @param contData
    * @return
    */
  def computeContMean(pCOverX: RDD[(Long, Array[Array[Double]])],
                      contData: RDD[(Long, Vector[Double])]): Array[Array[Vector[Double]]] = {
    val denumerator: Array[Array[Double]] = pCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    var numerator: Array[Array[Vector[Double]]] = contData.join(pCOverX).mapValues(v => {
      val x: Vector[Double] = v._1
      val p: Array[Array[Double]] = v._2
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield x * p(row)(col) // x * p(c / x)
          ).toArray
      temp.toArray
    }).map(_._2).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    val t = for (row <- 0 until AppContext.gridSize._1)
      yield (
        for (col <- 0 until AppContext.gridSize._2)
          yield numerator(row)(col) / denumerator(row)(col)
        ).toArray
    t.toArray
  }

  /**
    * Compute the Standard deviation for continuous variable
    *
    * @param pCOverX
    * @param contData
    * @param contMean
    * @return
    */
  def computeContStd(pCOverX: RDD[(Long, Array[Array[Double]])],
                     contData: RDD[(Long, Vector[Double])],
                     contMean: Array[Array[Vector[Double]]]
                    ): Array[Array[Double]] = {
    val denumerator: Array[Array[Double]] = pCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    }).map(_.map(_ * AppContext.contSize))

    var numerator: Array[Array[Double]] = contData.join(pCOverX).mapValues(v => {
      val x: Vector[Double] = v._1
      val p: Array[Array[Double]] = v._2
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield scala.math.pow(norm(contMean(row)(col) - x), 2) * p(row)(col) // x * p(c / x)
          ).toArray
      temp.toArray
    }).map(_._2).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    val t = for (row <- 0 until AppContext.gridSize._1)
      yield (
        for (col <- 0 until AppContext.gridSize._2)
          yield numerator(row)(col) / denumerator(row)(col)
        ).toArray
    t.toArray
  }

  def computeBinMean(pCOverX: RDD[(Long, Array[Array[Double]])], binData: RDD[(Long, Vector[Int])]): Array[Array[DenseVector[Int]]] = {
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

  def computeBinStd(pCOverX: RDD[(Long, Array[Array[Double]])],
                    binMean: Array[Array[DenseVector[Int]]],
                    binData: RDD[(Long, Vector[Int])]): Array[Array[Double]] = {
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
        data.map(_.map(_ * AppContext.binSize))
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


