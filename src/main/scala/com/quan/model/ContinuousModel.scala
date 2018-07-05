package com.quan.model

import breeze.linalg._
import com.quan.util.DistributionHelper
import org.apache.spark.rdd.RDD

class ContinuousModel(val numRows: Int, val numCols: Int) extends Serializable {
  def pXOverC(
               contData: RDD[(Long, Vector[Double])],
               cells: Array[Array[Cell]]):
  RDD[(Long, Array[Array[Double]])] = {
    println("Cont Model: pXOverC")
    contData.mapValues(x => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield DistributionHelper.normalLog(x, cells(row)(col).contMean, cells(row)(col).contStd)
          ).toArray
      temp.toArray
    })
  }

  /**
    * compute the mean for continuous data w(c)
    *
    * @param pCOverX
    * @param contData
    * @return
    */
  def mean(pCOverX: RDD[(Long, Array[Array[Double]])],
           contData: RDD[(Long, Vector[Double])]): Array[Array[Vector[Double]]] = {
    println("Cont Model: mean")

    val v = pCOverX.collect()

    val denumerator: Array[Array[Double]] = pCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    var numerator: Array[Array[Vector[Double]]] = contData.join(pCOverX).mapValues(v => {
      val x: Vector[Double] = v._1
      val p: Array[Array[Double]] = v._2
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield x * p(row)(col) // x * p(c / x)
          ).toArray
      temp.toArray
    }).map(_._2).reduce((v1, v2) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    val t = for (row <- 0 until numRows)
      yield (
        for (col <- 0 until numCols)
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
  def std(pCOverX: RDD[(Long, Array[Array[Double]])],
          contData: RDD[(Long, Vector[Double])],
          contMean: Array[Array[Vector[Double]]],
          contSize: Int
         ): Array[Array[Double]] = {
    println("Cont Model: std")
    val denumerator: Array[Array[Double]] = pCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    }).map(_.map(_ * contSize))

    var numerator: Array[Array[Double]] = contData.join(pCOverX).mapValues(v => {
      val x: Vector[Double] = v._1
      val p: Array[Array[Double]] = v._2
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield scala.math.pow(norm(contMean(row)(col) - x), 2) * p(row)(col) // x * p(c / x)
          ).toArray
      temp.toArray
    }).map(_._2).reduce((v1, v2) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    val t = for (row <- 0 until numRows)
      yield (
        for (col <- 0 until numCols)
          yield numerator(row)(col) / denumerator(row)(col)
        ).toArray
    t.toArray
  }
}
