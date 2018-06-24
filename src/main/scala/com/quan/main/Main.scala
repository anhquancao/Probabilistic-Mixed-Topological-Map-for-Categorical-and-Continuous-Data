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


    val pXOverC: RDD[(Long, Array[Array[Double]])] = SOMHelper.computePXOverC(binData, contData, cells)
    //   val a = pXBinOverC.take(3)
    val test = pXOverC.take(3)

    var iter: Int = 0

    //    println(DistributionHelper.gaussian(DenseVector(80.0, 82.0), DenseVector(74.0, 81.0), 7.0))
    //    println(DistributionHelper.hammingDistance(DenseVector(0,1,0,0,1), DenseVector(1,1,0,1,0)))

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
