package com.quan.main

import breeze.linalg._
import com.quan.model.{Cell, MixedModel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val r: RDD[Vector[Double]] = Reader.read("src/resources/mfeat-kar-mini.txt", "[ \t]+")
      .map(arr => new DenseVector[Double](arr.map(_.toDouble)))

    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits-mini.csv", ",")
      .map(arr => new DenseVector[Int](arr.map(_.toInt)))

    //    AppContext.contSize = r.take(1)(0).size // size of continuous part
    //    AppContext.binSize = b.take(1)(0).size // size of binary part

    // Add index to the binary and continous data
    val binData: RDD[(Long, Vector[Int])] = b.zipWithIndex().map(t => (t._2, t._1))
    val contData: RDD[(Long, Vector[Double])] = r.zipWithIndex().map(t => (t._2, t._1))

    val model = new MixedModel(5, 5)
    val cells: Array[Array[Cell]] = model.train(binData, contData)
    val test = 1
  }
}
