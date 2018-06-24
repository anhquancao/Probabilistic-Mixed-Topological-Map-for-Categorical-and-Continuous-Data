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

  def computeT(iter: Int): Double = {
    AppContext.TMax *
      scala.math.pow(
        AppContext.TMin / AppContext.TMax,
        iter / AppContext.maxIter
      )
  }
}
