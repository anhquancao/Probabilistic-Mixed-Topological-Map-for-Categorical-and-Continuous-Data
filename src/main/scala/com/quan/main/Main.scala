package com.quan.main

import breeze.linalg._
import com.quan.model.{Cell, MixedModel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {



  def normalizeData(data: RDD[Vector[Double]]): RDD[Vector[Double]] = {
    val maxVector: Vector[Double] = data.reduce((v1: Vector[Double], v2: Vector[Double]) => {
      for (i <- 0 until v1.length) {
        if (v2(i) > v1(i))
          v1(i) = v2(i)
      }
      v1
    })
    val minVector: Vector[Double] = data.reduce((v1: Vector[Double], v2: Vector[Double]) => {
      for (i <- 0 until v1.length) {
        if (v2(i) < v1(i))
          v1(i) = v2(i)
      }
      v1
    })

    val normalizedData: RDD[Vector[Double]] = data.map((v: Vector[Double]) => {
      for (i <- 0 until v.length) {
        v(i) = (v(i) - minVector(i)) / (maxVector(i) - minVector(i))
      }
      v
    })
    //    val normData = normalizedData.collect()
    normalizedData
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val maxIter = 30

    val numRows: Int = 5
    val numCols: Int = 5

    val dataSize: Int = 500

    val r: RDD[Vector[Double]] = Reader.read("src/resources/s1.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))


    val normalizedR = normalizeData(r)

    val test = normalizedR.take(10)

    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits.csv", ",")
      .map(arr => new DenseVector[Int](arr.map(_.toInt)))

    //    AppContext.contSize = r.take(1)(0).size // size of continuous part
    //    AppContext.binSize = b.take(1)(0).size // size of binary part

    // Add index to the binary and continous data
    val binData: RDD[(Long, Vector[Int])] = b.zipWithIndex().map(t => (t._2, t._1)).filter(_._1 < dataSize)
    val contData: RDD[(Long, Vector[Double])] = normalizedR.zipWithIndex().map(t => (t._2, t._1)).filter(_._1 < dataSize)

    val model = new MixedModel(numRows, numCols)
    val cells: Array[Array[Cell]] = model.train(binData, contData, maxIter)



  }
}
