package com.quan.model

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.DistributionHelper
import org.apache.spark.rdd.RDD

object ContinuousModel {
  def pXOverC(contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    contData.mapValues(x => {
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield DistributionHelper.gaussian(x, cells(row)(col).contMean, cells(row)(col).contStd)
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
  def std(pCOverX: RDD[(Long, Array[Array[Double]])],
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
}
