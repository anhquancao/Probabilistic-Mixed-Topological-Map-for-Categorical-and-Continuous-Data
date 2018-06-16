package com.quan.main

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.util.{DistributionHelper, RandomHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val r: RDD[Vector[Double]] = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))

    val b: RDD[Vector[Double]] = Reader.read("src/resources/digits.csv", ",")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))

    val n = r.take(1)(0).size // size of continuous part
    val m = b.take(1)(0).size // size of binary part
    println("r size: " + r.count() + " x " + r.take(1)(0).size)
    println("b size: " + b.count() + " x " + b.take(1)(0).size)

    val NIter: Int = 100
    val TMax = 10
    val TMin = 1
    var t = 0

    // continuous parameters
    var RSignma = AppContext.getRandom.nextDouble
    var RStd = AppContext.getRandom.nextDouble
    var RMean = RandomHelper.createRandomDoubleVector(n)

    // binary parameters
    var BMean = RandomHelper.createRandomBinaryVector(m)
    var BEpsilon = AppContext.getRandom.nextDouble() / 2

    println("B Mean: " + BMean.size)
    println(BMean)
    println("R Mean: " + RMean.size)
    println(RMean)
    println("BEpsilon: " + BEpsilon)

//    println(DistributionHelper.gaussian(DenseVector(80.0, 82.0), DenseVector(74.0, 81.0), 7.0))
//    println(DistributionHelper.hammingDistance(DenseVector(0,1,0,0,1), DenseVector(1,1,0,1,0)))

    while (t < NIter) {
      t += 1
      val T: Double = TMax * scala.math.pow(TMin / TMax, t / (NIter))
    }
  }
}
