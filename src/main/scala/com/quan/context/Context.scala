package com.quan.context

import org.apache.spark.{SparkConf, SparkContext}

object Context {
  private var sc: Option[SparkContext] = None

  def getSparkContext: SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PrMTM")
    if (this.sc.isEmpty) {
      sc = Some(new SparkContext(conf))
    }
    return sc.get
  }

}
