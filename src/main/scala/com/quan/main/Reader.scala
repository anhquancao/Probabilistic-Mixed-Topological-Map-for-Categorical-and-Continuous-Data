package com.quan.main

import com.quan.context.AppContext
import org.apache.spark.rdd.RDD

object Reader {
  def read(fileName: String, delimiter: String): RDD[Array[String]] = {
    val sc = AppContext.getSparkContext;
    val textFile = sc.textFile(fileName)
    val r = textFile.map((lines) => {
      lines.trim().split(delimiter)
    })
    r
  }
}
