package com.quan.main

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.model.{BinaryModel, Cell, ContinuousModel, MixedModel}
import com.quan.util.{DistributionHelper, RandomHelper}
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

    val cells: Array[Array[Cell]] = MixedModel.createCells(AppContext.gridSize._1, AppContext.gridSize._2)

    // compute p(x/c)
    val pXOverC: RDD[(Long, Array[Array[Double]])] = MixedModel.pXOverC(binData, contData, cells)

    val T: Double = MixedModel.T(2)

    // compute p(x)
    val pX: RDD[(Long, Double)] = MixedModel.pX(cells, pXOverC, T)

    // compute p(c/x)
    val pCOverX: RDD[(Long, Array[Array[Double]])] = MixedModel.pCOverX(pX, pXOverC, cells, T)

    // compute p(c) from p(c/x)
    val pC: Array[Array[Double]] = MixedModel.pC(pCOverX)

    // compute the mean for continuous data
    val contMean: Array[Array[Vector[Double]]] = ContinuousModel.mean(pCOverX, contData)

    // compute continuous standard deviation
    val contStd = ContinuousModel.std(pCOverX, contData, contMean)


    val binMean: Array[Array[DenseVector[Int]]] = BinaryModel.mean(pCOverX, binData)

    val binStd = BinaryModel.std(pCOverX, binMean, binData)

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
