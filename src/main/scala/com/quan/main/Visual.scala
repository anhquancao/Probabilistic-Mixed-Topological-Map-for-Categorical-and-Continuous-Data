package com.quan.main

import breeze.linalg.{DenseVector, Vector}
import com.quan.model.MixedModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object Visual {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val r: RDD[Vector[Double]] = Reader.read("src/resources/s1.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))
    val test = r.take(10)
    val model = new MixedModel(5, 5)
//    for (i <- 0 until 5) {
//      println(model.getT(i, 5))
//    }
  }
}
