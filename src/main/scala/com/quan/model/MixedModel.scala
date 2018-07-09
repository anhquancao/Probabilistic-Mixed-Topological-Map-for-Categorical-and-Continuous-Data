package com.quan.model

import breeze.linalg._
import com.quan.util.{DistributionHelper, RandomHelper}
import org.apache.spark.rdd.RDD

class MixedModel(numRows: Int, numCols: Int, TMin: Int = 1, TMax: Int = 10) extends Serializable {

  val binaryModel = new BinaryModel(numRows, numCols)
  val continuousModel = new ContinuousModel(numRows, numCols)

  /**
    *
    * @return
    */
  def createCells(contSize: Int, binSize: Int,
                  binData: RDD[(Long, Vector[Int])],
                  contData: RDD[(Long, Vector[Double])]): Array[Array[Cell]] = {

    println("Mixed mode: Create cells")

    //    var contMean: Vector[Double] = contData.map(_._2).reduce((v1, v2) => v1 + v2).map(_ / contSize)
    var contMean: Vector[Double] = RandomHelper.createRandomDoubleVector(contSize)

    var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(binSize)

    val prob = 1.0 / (numCols * numRows)
    val temp = for (row <- 0 to numRows)
      yield (
        for (col <- 0 to numCols)
          yield new Cell(row, col, contSize, binSize, prob, contMean, binMean)
        ).toArray
    temp.toArray
  }

  def logPXOverC(binData: RDD[(Long, Vector[Int])], contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {

    println("Mixed model: Compute pXOverC")

    // compute the bernouli of x over c
    val logPXBinOverC: RDD[(Long, Array[Array[Double]])] = this.binaryModel.logPXOverC(binData, cells)

    // compute gaussian
    val logPXContOverC: RDD[(Long, Array[Array[Double]])] = this.continuousModel.logPXOverC(contData, cells)

    //    val b = pXBinOverC.take(3)

    // compute the p(x/c)
    val logPXOverC = logPXBinOverC.join(logPXContOverC).map((p: (Long, (Array[Array[Double]], Array[Array[Double]]))) => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield p._2._1(row)(col) + p._2._2(row)(col)
          ).toArray
      (p._1, temp.toArray)
    })

    logPXOverC
  }

  //  var count = 0
  // compute p(c/c*)
  def pCOverCStar(c: (Int, Int), cStar: (Int, Int), T: Double): Double = {
    //    count += 1
    //    println("Mixed model: Compute pCOverCStar " + count)
    var pCOverCStartSum = 0.0
    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        val r = (row, col)
        pCOverCStartSum = pCOverCStartSum + DistributionHelper.kernel(DistributionHelper.distance(cStar, r), T)
      }
    }

    DistributionHelper.kernel(DistributionHelper.distance(c, cStar), T) / pCOverCStartSum
  }


  // compute p(x)
  def logPX(cells: Array[Array[Cell]], logPXOverC: RDD[(Long, Array[Array[Double]])], T: Double): RDD[(Long, Double)] = {
    println("Mixed model: Compute pX")
    logPXOverC.mapValues(v => {

      var logPXs: Array[Double] = Array()

      for (rowStar <- 0 until numRows) {
        for (colStar <- 0 until numCols) {

          // get c*
          val cStar = (rowStar, colStar)

          // p(c*)
          val pCStar = cells(rowStar)(colStar).prob

          var maxLogVal: Double = Double.MinValue

          var logVals: Array[Double] = Array()

          for (row <- 0 until numRows) {
            for (col <- 0 until numCols) {
              // get c
              val c = (row, col)

              // p(x/c)
              val logPXOverCValue = v(row)(col)

              // p(c/c*)
              val pCOverCStar = this.pCOverCStar(c, cStar, T)

              // p(x/c*) = sum p(x / c) * p(c/c*)
              val logVal = logPXOverCValue + scala.math.log(pCOverCStar)

              if (logVal > maxLogVal) maxLogVal = logVal

              logVals = logVals :+ logVal
              //              pXOverCStar += scala.math.exp(logVal)
            }
          }

          val logPXOverCStar: Double = maxLogVal + scala.math.log(logVals.map(v => scala.math.exp(v - maxLogVal)).sum)

          // p(x) = p(c*) x p(x/c*)
          val logPx: Double = scala.math.log(pCStar) + logPXOverCStar

          logPXs = logPXs :+ logPx
        }
      }

      val maxLogPxItem: Double = logPXs.max
      val logPX: Double = maxLogPxItem + scala.math.log(logPXs.map(v => scala.math.exp(v - maxLogPxItem)).sum)
      logPX
    })
  }

  // compute p(c/x)
  def pCOverX(pX: RDD[(Long, Double)],
              logPXOverC: RDD[(Long, Array[Array[Double]])],
              cells: Array[Array[Cell]],
              T: Double): RDD[(Long, Array[Array[Double]])] = {

    println("Mixed model: Compute pCOverX")
    pX.join(logPXOverC).map(v => {
      val logPX: Double = v._2._1
      val pxOverC: Array[Array[Double]] = v._2._2

      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield 0.0
          ).toArray

      val pCOverXArr: Array[Array[Double]] = temp.toArray

      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {

          // get c*
          val c = (row, col)

          for (rowStar <- 0 until numRows) {
            for (colStar <- 0 until numCols) {

              // get c*
              val cStar = (rowStar, colStar)

              val pCStar: Double = cells(rowStar)(colStar).prob


              // p(c/c*)
              val pCOverCStar: Double = this.pCOverCStar(c, cStar, T)

              // p(c, c*/ x)
              val logPCAndCStar: Double = scala.math.log(pCOverCStar) + scala.math.log(pCStar) + scala.math.log(pxOverC(row)(col)) - logPX

              pCOverXArr(row)(col) += scala.math.exp(logPCAndCStar)

            }
          }

        }
      }
      (v._1, pCOverXArr)
    })
  }

  /**
    * p(c)
    *
    * @param logPCOverX
    * @return
    */
  def logPC(logPCOverX: RDD[(Long, Array[Array[Double]])]): Array[Array[Double]] = {
    println("Mixed model: Compute pC")
    val maxPCOverX = logPCOverX.map(_._2).reduce((v1, v2) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          if (v2(row)(col) > v1(row)(col))
            v1(row)(col) = v2(row)(col)
        }
      }
      v1
    })
    val t = logPCOverX.map(_._2).map((v: Array[Array[Double]]) => {
      // exp(a_i - b)
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v(row)(col) -= maxPCOverX(row)(col)
        }
      }
      v
    }).reduce((v1: Array[Array[Double]], v2: Array[Array[Double]]) => {
      // compute sum exp(a_i - b)
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row)(col) += v2(row)(col)
        }
      }
      v1
    })
    t.map(_.map(_ / (numRows * numCols)))
  }

  def getT(iteration: Int, maxIteration: Int): Double = {
    println("Mixed model: Compute T")
    TMax *
      scala.math.pow(
        TMin / TMax,
        iteration / maxIteration
      )
  }

  def train(binData: RDD[(Long, Vector[Int])],
            contData: RDD[(Long, Vector[Double])],
            maxIteration: Int = 10
           ): Array[Array[Cell]] = {
    var iteration: Int = 0

    val contSize = contData.take(1)(0)._2.size
    val binSize = binData.take(1)(0)._2.size

    var cells: Array[Array[Cell]] =
      createCells(contSize, binSize, binData, contData)


    while (iteration < maxIteration) {
      iteration += 1

      println("Iteration: " + iteration)

      val T: Double = getT(iteration, maxIteration)

      // compute p(x/c)
      val logPXOverC: RDD[(Long, Array[Array[Double]])] = this.logPXOverC(binData, contData, cells)

      // compute p(x)
      val logPX: RDD[(Long, Double)] = this.logPX(cells, logPXOverC, T)

      val t = logPX.collect()

      // compute p(c/x)
      val pCOverX: RDD[(Long, Array[Array[Double]])] = this.pCOverX(logPX, logPXOverC, cells, T)

      // compute p(c) from p(c/x)
      val pC: Array[Array[Double]] = this.logPC(pCOverX)

      // compute the mean for continuous data
      val contMean: Array[Array[Vector[Double]]] = this.continuousModel.mean(pCOverX, contData)

      // compute continuous standard deviation
      val contStd = this.continuousModel.std(pCOverX, contData, contMean, contSize)


      val binMean: Array[Array[DenseVector[Int]]] = this.binaryModel.mean(pCOverX, binData)

      val binStd = this.binaryModel.std(pCOverX, binMean, binData, binSize)

      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          cells(row)(col).contMean = contMean(row)(col)
          cells(row)(col).contStd = contStd(row)(col)
          cells(row)(col).binMean = binMean(row)(col)
          cells(row)(col).binStd = binStd(row)(col)
          cells(row)(col).prob = pC(row)(col)
        }
      }

    }
    cells
  }
}
