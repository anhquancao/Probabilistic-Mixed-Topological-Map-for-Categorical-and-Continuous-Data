package com.quan.main

import com.quan.context.AppContext
import com.quan.context.util.RandomHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val r: RDD[Vector[Double]] = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+")
      .map(_.map(_.toDouble).to[Vector])
    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits.csv", ",")
      .map(_.map(_.toInt).to[Vector])
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

//    println("B Mean: " + BMean.size)
//    println("R Mean: " + RMean.size)

//    println("BEpsilon: " + BEpsilon)

    while (t < NIter) {
      t += 1
      val T = TMax * scala.math.pow(TMin / TMax, t / (NIter))
    }
  }
}
