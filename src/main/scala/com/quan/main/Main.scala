package com.quan.main

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.model.Cell
import com.quan.util.{DistributionHelper, RandomHelper, SOMHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {
  def computePXContOverC(cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    AppContext.getContData.mapValues(x => {
      val temp = for (row <- 0 to AppContext.gridSize._1)
        yield (
          for (col <- 0 to AppContext.gridSize._2)
            yield DistributionHelper.gaussian(x, cells(row)(col).contMean, cells(row)(col).contStd)
          ).toArray
      temp.toArray
    })
  }

  def computePXBinOverC(cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    AppContext.getBinData.mapValues(x => {
      val temp = for (row <- 0 to AppContext.gridSize._1)
        yield (
          for (col <- 0 to AppContext.gridSize._2)
            yield DistributionHelper.bernouli(x, cells(row)(col).binMean, cells(row)(col).binEpsilon)
          ).toArray
      temp.toArray
    })
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val r: RDD[Vector[Double]] = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))

    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits.csv", ",")
      .map(arr => new DenseVector[Int](arr.map(_.toInt)))

    AppContext.contSize = r.take(1)(0).size // size of continuous part
    AppContext.binSize = b.take(1)(0).size // size of binary part

    // Add index to the binary and continous data
    AppContext.binData = Some(b.zipWithIndex().map(t => (t._2, t._1)))
    AppContext.contData = Some(r.zipWithIndex().map(t => (t._2, t._1)))

    AppContext.dataSize = b.count()
    AppContext.pX = RandomHelper.createRandomDoubleVector(AppContext.dataSize)

    val cells: Array[Array[Cell]] = SOMHelper.createCells(AppContext.gridSize._1, AppContext.gridSize._2)

    // compute the bernouli of x over c
    val pXBinOverC: RDD[(Long, Array[Array[Double]])] = computePXBinOverC(cells)

    // compute gaussian
    val pXContOverC: RDD[(Long, Array[Array[Double]])] = computePXContOverC(cells)

    //   val a = pXBinOverC.take(3)
    val test = pXContOverC.take(3)

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
