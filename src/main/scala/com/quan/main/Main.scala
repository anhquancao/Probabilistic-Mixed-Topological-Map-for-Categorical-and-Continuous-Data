package com.quan.main

import breeze.linalg._
import com.quan.context.AppContext
import com.quan.model.{Cell, MixedModel}
import com.quan.util.IOHelper
import index.{ExternalIndex, InternalIndex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Main {


  def normalizeData(data: RDD[Vector[Double]]): RDD[Vector[Double]] = {
    val maxVector: Vector[Double] = data.reduce((v1: Vector[Double], v2: Vector[Double]) => {
      for (i <- 0 until v1.length) {
        if (v2(i) > v1(i))
          v1(i) = v2(i)
      }
      v1
    })
    val minVector: Vector[Double] = data.reduce((v1: Vector[Double], v2: Vector[Double]) => {
      for (i <- 0 until v1.length) {
        if (v2(i) < v1(i))
          v1(i) = v2(i)
      }
      v1
    })

    val normalizedData: RDD[Vector[Double]] = data.map((v: Vector[Double]) => {
      for (i <- 0 until v.length) {
        v(i) = (v(i) - minVector(i)) / (maxVector(i) - minVector(i))
      }
      v
    })
    //    val normData = normalizedData.collect()
    normalizedData
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val maxIter = 30

    val numRows: Int = 6
    val numCols: Int = 6

    val dataSize: Int = 2000

    val r: RDD[Vector[Double]] = Reader.read("src/resources/d31/d31.csv", ",")
      .map(arr => new DenseVector[Double](arr.slice(1, arr.length).map(_.toDouble)))


    val normalizedR = normalizeData(r)


    val b: RDD[Vector[Int]] = Reader.read("src/resources/digits.csv", ",")
      .map(arr => new DenseVector[Int](arr.map(_.toInt)))


    // Add index to the binary and continous data
    val binData: RDD[(Long, Vector[Int])] = b.zipWithIndex().map(t => (t._2, t._1)).filter(_._1 < dataSize)
    val contData: RDD[(Long, Vector[Double])] = normalizedR.zipWithIndex().map(t => (t._2, t._1)).filter(_._1 < dataSize)

    val trueLabels: RDD[String] = Reader.read("src/resources/d31/labels.csv", ",")
      .map(_.head)
      .zipWithIndex()
      .filter(_._2 < dataSize)
      .map(_._1)

    val model = new MixedModel(numRows, numCols)
    val cells: Array[Array[Cell]] = model.train(binData, contData, maxIter)

    val predictedLabels: RDD[String] = model.predictedTrainingLabels.sortByKey().map(_._2 + "")

    //    val predictedLabelsCollect = model.predictedTrainingLabels.sortByKey().take(10)

    val intDirName = model.dirName + "/InternalIndices.txt"

    val points: RDD[Array[Double]] = contData.join(binData).map((v: (Long, (Vector[Double], Vector[Int]))) => {
      val a1: Array[Double] = v._2._1.toArray
      val a2: Array[Double] = v._2._2.toArray.map(_.toDouble)
      val arr: Array[Double] = a1 ++ a2
      arr
    })


    val internalIndex = new InternalIndex(points, predictedLabels, AppContext.getSparkContext)
    IOHelper.writeIndices(intDirName, "Silhouette: " + internalIndex.silhouette())
    IOHelper.writeIndices(intDirName, "Davies Bouldin: " + internalIndex.davies_bouldin())

    val externalIndex = new ExternalIndex(trueLabels, predictedLabels)

    val extDirName = model.dirName + "/ExternalIndices.txt"
    IOHelper.writeIndices(extDirName, "Czekanowski Dice: " + externalIndex.czekanowskiDice())
    IOHelper.writeIndices(extDirName, "Folkes Mallows: " + externalIndex.folkesMallows())
    IOHelper.writeIndices(extDirName, "NMI: " + externalIndex.nmi())
    IOHelper.writeIndices(extDirName, "Precision: " + externalIndex.precision())
    IOHelper.writeIndices(extDirName, "Recall: " + externalIndex.recall())
    IOHelper.writeIndices(extDirName, "Rand: " + externalIndex.rand())



  }
}
