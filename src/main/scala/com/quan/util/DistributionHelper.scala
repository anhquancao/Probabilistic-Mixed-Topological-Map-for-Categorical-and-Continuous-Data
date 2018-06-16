package com.quan.util

import breeze.linalg._


object DistributionHelper {

  def gaussian(x: Vector[Double], mean: Vector[Double], std: Double): Double = {
    val normSquare: Double = scala.math.pow(norm(x - mean), 2.0)
    val t: Double = 2 * scala.math.Pi * std
    val leftDenum: Double = scala.math.pow(t, x.size / 2.0)
    val left: Double = 1.0 / leftDenum
    val right: Double = scala.math.exp(-1.0 * normSquare / (2 * scala.math.pow(std, 2)))
    left * right
  }
}
