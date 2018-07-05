package com.quan.util

import breeze.linalg._


object DistributionHelper {
  def hammingDistance(xi: Vector[Int], xj: Vector[Int]): Double = {
    val diff: Vector[Int] = xi - xj
    sum(diff.map(v => v.abs))
  }

  def normalLog(x: Vector[Double], mean: Vector[Double], std: Double): Double = {
    var stdVar = std
    val zero = 1.0e-38
    if (!(std * std > 0)) {
      stdVar = zero
    }
    val diffVec = x - mean
    val normValue = norm(diffVec)
    val res = -x.size / 2 * scala.math.log(2 * scala.math.Pi) - x.size / 2 * scala.math.log(stdVar) - 0.5 * scala.math.pow(normValue / stdVar, 2.0)
    res
  }

  def gaussian(x: Vector[Double], mean: Vector[Double], std: Double): Double = {
    val normSquare: Double = scala.math.pow(norm(x - mean), 2.0)
    val t: Double = 2 * scala.math.Pi * std
    val leftDenum: Double = scala.math.pow(t, x.size / 2.0)
    val left: Double = 1.0 / leftDenum
    val right: Double = scala.math.exp(-1.0 * normSquare / (2 * scala.math.pow(std, 2)))
    val res = left * right
    res
  }


  def distance(c1: (Int, Int), c2: (Int, Int)): Int = {
    scala.math.abs(c1._1 - c2._1) + scala.math.abs(c1._2 - c2._2)
  }

  def kernel(distance: Double, T: Double): Double = {
    scala.math.exp(-0.5 * distance / T)
  }

  def bernouli(x: Vector[Int], mean: Vector[Int], epsilon: Double): Double = {
    val hamming = DistributionHelper.hammingDistance(x, mean)
    scala.math.log(scala.math.pow(epsilon, hamming)) + scala.math.log(scala.math.pow(1 - epsilon, x.size - hamming))
  }
}
