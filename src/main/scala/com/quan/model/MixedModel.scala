package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.DistributionHelper
import org.apache.spark.rdd.RDD

object MixedModel {
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

  def pXOverC(binData: RDD[(Long, Vector[Int])], contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    // compute the bernouli of x over c
    val pXBinOverC: RDD[(Long, Array[Array[Double]])] = BinaryModel.pXOverC(binData, cells)

    // compute gaussian
    val pXContOverC: RDD[(Long, Array[Array[Double]])] = ContinuousModel.pXOverC(contData, cells)

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
  def pX(cells: Array[Array[Cell]], pXOverC: RDD[(Long, Array[Array[Double]])], T: Double): RDD[(Long, Double)] = {
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
              val pCOverCStar = MixedModel.pCOverCStar(c, cStar, T)

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
  def pCOverX(pX: RDD[(Long, Double)],
              pXOverC: RDD[(Long, Array[Array[Double]])],
              cells: Array[Array[Cell]],
              T: Double): RDD[(Long, Array[Array[Double]])] = {
    pX.join(pXOverC).map(v => {
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
              val pCOverCStar: Double = MixedModel.pCOverCStar(c, cStar, T)

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
  def pC(pCOverX: RDD[(Long, Array[Array[Double]])]): Array[Array[Double]] = {
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

  def T(iter: Int): Double = {
    AppContext.TMax *
      scala.math.pow(
        AppContext.TMin / AppContext.TMax,
        iter / AppContext.maxIter
      )
  }
}
