package com.quan.main

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.model.Cell
import com.quan.util.{DistributionHelper, RandomHelper, SOMHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val r: RDD[Vector[Double]] = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))

    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits.csv", ",")
      .map(arr => new DenseVector[Int](arr.map(_.toInt)))

    AppContext.contSize = r.take(1)(0).size // size of continuous part
    AppContext.binSize = b.take(1)(0).size // size of binary part

    // Add index to the binary and continous data
    val binData: RDD[(Long, Vector[Int])] = b.zipWithIndex().map(t => (t._2, t._1))
    val contData: RDD[(Long, Vector[Double])] = r.zipWithIndex().map(t => (t._2, t._1))

    AppContext.dataSize = b.count()
    AppContext.pX = RandomHelper.createRandomDoubleVector(AppContext.dataSize)

    val cells: Array[Array[Cell]] = SOMHelper.createCells(AppContext.gridSize._1, AppContext.gridSize._2)

    // compute p(x/c)
    val pXOverC: RDD[(Long, Array[Array[Double]])] = SOMHelper.computePXOverC(binData, contData, cells)

    val T: Double = SOMHelper.computeT(2)

    // compute p(x)
    val pX: RDD[(Long, Double)] = SOMHelper.computePX(cells, pXOverC, T)

    // compute p(c/x)
    val pCOverX: RDD[(Long, Array[Array[Double]])] = SOMHelper.computePCOverX(pX, pXOverC, cells, T)

    // compute p(c) from p(c/x)
    //    val pC: Array[Array[Double]] = SOMHelper.computePC(pCOverX)

    // compute the mean for continuous data
    //    val contMean: Array[Array[Vector[Double]]] = SOMHelper.computeContMean(pCOverX, contData)

    // compute continuous standard deviation
    //    val contStd = SOMHelper.computeContStd(pCOverX, contData, contMean)


    val leftPart: Array[Array[Vector[Double]]] = pCOverX.join(binData).map(v => {
      val x: Vector[Double] = new DenseVector[Double](v._2._2.toArray.map(_.toDouble))
      val pC: Array[Array[Double]] = v._2._1
      val temp = for (row <- 0 until AppContext.gridSize._1)
        yield (
          for (col <- 0 until AppContext.gridSize._2)
            yield (1.0 - x) * pC(row)(col)
          ).toArray
      temp.toArray
    }).reduce((v1, v2) => {
      for (row <- 0 until AppContext.gridSize._1) {
        for (col <- 0 until AppContext.gridSize._2) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })

    val test = leftPart.take(2)


    var iter: Int = 0


    while (iter < AppContext.maxIter) {
      iter += 1
      println("Iter: " + iter)
      //      for (row <- 0 to AppContext.gridSize._1) {
      //        for (col <- 0 to AppContext.gridSize._2) {
      //          println(cells(row)(col))
      //        }
      //      }
    }
  }
}
