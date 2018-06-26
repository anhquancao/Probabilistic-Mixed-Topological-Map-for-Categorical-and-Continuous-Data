package com.quan.context

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg._
import com.quan.util.RandomHelper

import scala.util.Random

object AppContext {

  var contSize: Int = 0
  var binSize: Int = 0
  var dataSize:Long = 0
  var gridSize: (Int, Int) = (5, 5) // (num_rows, num_cols)
  val numberCells = 25
  val TMax: Int = 10
  val TMin: Int = 1
  val maxIter: Int = 10
  var pX :Vector[Double] = RandomHelper.createRandomDoubleVector(AppContext.dataSize)

  private var sc: Option[SparkContext] = None
  private var random: Option[Random] = None

  def getSparkContext: SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PrMTM")
    if (this.sc.isEmpty) {
      sc = Some(new SparkContext(conf))
    }
    sc.get
  }

  def getRandom: Random = {
    if (this.random.isEmpty) {
      random = Some(new Random(1))
    }
    random.get
  }
}
