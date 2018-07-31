package com.quan.context

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object AppContext {

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
//      random = Some(new Random(1))
      random = Some(new Random(1))
    }
    random.get
  }
}
