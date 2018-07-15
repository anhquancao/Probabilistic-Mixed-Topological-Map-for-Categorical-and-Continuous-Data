package com.quan.util

import breeze.linalg._


object DistributionHelper {
  def hammingDistance(xi: Vector[Int], xj: Vector[Int]): Double = {
    val diff: Vector[Int] = xi - xj
    sum(diff.map(v => v.abs))
  }

  def normalLog(x: Vector[Double], mean: Vector[Double], std: Double): Double = {
    //    var stdVar = std
    //    val zero = 1.0e-38
    //    if (!(std * std > 0)) {
    //      stdVar = zero
    //    }
    val diffVec = x - mean
    val s = x.length
    val normValue = norm(diffVec) / s
    val res = -1.0 * s / 2 * scala.math.log(2 * scala.math.Pi * std) - 0.5 * scala.math.pow(normValue / std, 2.0)
    res
  }

  def normalMultivariateLog(x: Vector[Double], mean: Vector[Double], std: Double): Double = {
    //    var stdVar = std
    //    val zero = 1.0e-38
    //    if (!(std * std > 0)) {
    //      stdVar = zero
    //    }
    val diffVec: Vector[Double] = x - mean
    val s: Int = x.length
    val covar: DenseMatrix[Double] = scala.math.pow(std, 2) * DenseMatrix.eye[Double](s)
    val covarInv: DenseMatrix[Double] = inv(covar)
    val normValue: Double = norm(diffVec) / s
//    val a = ncol(diffVec)
//    val b = dim(covarInv)
    val num1 = (covarInv.t * diffVec).t
    val numerator = -0.5 * num1 * diffVec
    val denumerator = 0.5 * s * scala.math.log(2 * scala.math.Pi) + 0.5 * scala.math.log(det(covar))
//    numerator - denumerator
    denumerator
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

  def logBernouli(x: Vector[Int], mean: Vector[Int], epsilon: Double): Double = {
    val hamming: Double = (DistributionHelper.hammingDistance(x, mean) * 1.0) / x.length
    val res = hamming * scala.math.log(epsilon) + (x.size - hamming) * scala.math.log(1 - epsilon)
    res
  }
}
