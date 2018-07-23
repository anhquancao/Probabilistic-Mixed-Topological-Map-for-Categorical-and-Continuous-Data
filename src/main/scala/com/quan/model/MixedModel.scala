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
    //    val p = logPXContOverC.take(20)
    //
    //    val b = logPXBinOverC.take(3)

    // compute the p(x/c)
    val logPXOverC = logPXBinOverC.join(logPXContOverC).map((p: (Long, (Array[Array[Double]], Array[Array[Double]]))) => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield p._2._1(row)(col) + p._2._2(row)(col)
          ).toArray
      (p._1, temp.toArray)
    })

    val test = logPXOverC.collect()

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

  // compute p(x/cStar)
  def logPXOverCStar(
                      logPXOverC: RDD[(Long, Array[Array[Double]])],
                      T: Double
                    ): RDD[(Long, Array[Array[Double]])] = {

    logPXOverC.mapValues((logPXOverCArr: Array[Array[Double]]) => {

      val logPXOverCStar: Array[Array[Double]] = RandomHelper.create2dArray(numRows, numCols, 0.0)
      for (rowStar <- 0 until numRows) {
        for (colStar <- 0 until numCols) {


          val cStar: (Int, Int) = (rowStar, colStar)

          val logValArr: Array[Array[Double]] = RandomHelper.create2dArray(numRows, numCols, 0.0)

          var maxLogVal: Double = Double.MinValue

          for (row <- 0 until numRows) {
            for (col <- 0 until numCols) {
              val c: (Int, Int) = (row, col)

              val logPCOverCStar: Double = this.pCOverCStar(c, cStar, T)

              // log p(x/cStar) = log p(x/c) + p(c/cStar)
              val logVal: Double = logPCOverCStar + logPXOverCArr(row)(col)

              logValArr(row)(col) = logVal

              if (logVal > maxLogVal) maxLogVal = logVal
            }
          }

          val expLogVal: Array[Array[Double]] = logValArr.map(_.map(v => scala.math.exp(v - maxLogVal)))
          val expSum: Double = expLogVal.map(_.sum).sum

          logPXOverCStar(rowStar)(colStar) = maxLogVal + scala.math.log(expSum)
        }
      }
      logPXOverCStar
    })
  }


  // compute p(x)
  def logPX(cells: Array[Array[Cell]], logPXOverCStar: RDD[(Long, Array[Array[Double]])], T: Double): RDD[(Long, Double)] = {
    println("Mixed model: Compute pX")
    logPXOverCStar.mapValues(v => {

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
              val logPXOverCStarValue = v(row)(col)

              // p(c/c*)
              val pCOverCStar = this.pCOverCStar(c, cStar, T)

              // p(x/c*) = sum p(x / c) * p(c/c*)
              val logVal = logPXOverCStarValue + scala.math.log(pCOverCStar)

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

  //  // p(cStar/x) =
  def logPCAndCStarOverX(
                          logPX: RDD[(Long, Double)],
                          cells: Array[Array[Cell]],
                          logPXOverC: RDD[(Long, Array[Array[Double]])],
                          T: Double
                        ): RDD[(Long, Array[Array[Double]])] = {
    val numCells: Int = numCols * numRows

    logPX.join(logPXOverC).mapValues(v => {
      val logPX: Double = v._1
      val logPXOverCArr: Array[Array[Double]] = v._2
      val res = RandomHelper.create2dArray(numCells, numCells, 0.0)

      for (rowStar <- 0 until numRows) {
        for (colStar <- 0 until numCols) {
          val logPCStar = scala.math.log(cells(rowStar)(colStar).prob)

          val cStar = (rowStar, colStar)

          for (row <- 0 until numRows) {
            for (col <- 0 until numCols) {
              val c = (row, col)
              val logPXOverC = logPXOverCArr(row)(col)
              val logPCOverCStar: Double = scala.math.log(this.pCOverCStar(c, cStar, T))

              res(rowStar * numCols + colStar)(row * numCols + col) = logPCOverCStar + logPXOverC + logPCStar - logPX
            }
          }
        }
      }
      res
    })
  }

  // compute p(cStar/x)
  // index = row * numCol + col
  def logPCStarOverX(logPCAndCStarOverX: RDD[(Long, Array[Array[Double]])]): RDD[(Long, Array[Double])] = {
    logPCAndCStarOverX.mapValues((logPCAndCStarOverX: Array[Array[Double]]) => {
      val maxLogCStar = logPCAndCStarOverX.map(_.max)

      val expSum: Array[Array[Double]] = logPCAndCStarOverX.map(_.zipWithIndex.map { case (s, i) => scala.math.exp(s - maxLogCStar(i)) })

      expSum.zipWithIndex.map { case (v, i) => maxLogCStar(i) + scala.math.log(v.sum) }
    })
  }

  // compute p(c/x)
  // index = row * numCol + col
  def logPCOverX(logPCAndCStarOverX: RDD[(Long, Array[Array[Double]])]): RDD[(Long, Array[Double])] = {

    val numCells: Int = numRows * numCols

    logPCAndCStarOverX.mapValues((logPCAndCStarOverX: Array[Array[Double]]) => {
      val maxLogC = (for (i <- 0 until numCells) yield Double.MinValue).toArray

      for (iStar <- 0 until numCells) {
        for (i <- 0 until numCells) {
          if (maxLogC(i) < logPCAndCStarOverX(iStar)(i)) {
            maxLogC(i) = logPCAndCStarOverX(iStar)(i)
          }
        }
      }
      val expSum: Array[Array[Double]] = logPCAndCStarOverX
        .map(
          _.zipWithIndex
            .map { case (v, i) => scala.math.exp(v - maxLogC(i)) }
        )

      val res = (for (i <- 0 until numCells) yield 0.0).toArray

      for (iStar <- 0 until numCells) {
        for (i <- 0 until numCells) {
          res(i) += expSum(iStar)(i)
        }
      }
      res.zipWithIndex.map { case (v, i) => maxLogC(i) + scala.math.log(v) }
    })

  }

  // p(c*)
  def logPCStar(logPCStarOverX: RDD[(Long, Array[Double])]): Array[Double] = {
    println("Mixed model: Compute pC")
    val maxLogPCOverX: Array[Double] = logPCStarOverX.map(_._2).reduce((v1: Array[Double], v2: Array[Double]) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          if (v2(row * numCols + col) > v1(row * numCols + col))
            v1(row * numCols + col) = v2(row * numCols + col)
        }
      }
      v1
    })
    val sumExp: Array[Double] = logPCStarOverX.map(_._2).map((v: Array[Double]) => {
      // exp(a_i - b) for each point
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v(row * numCols + col) = scala.math.exp(v(row * numCols + col) - maxLogPCOverX(row * numCols + col))
        }
      }
      v
    }).reduce((v1: Array[Double], v2: Array[Double]) => {
      // compute sum exp(a_i - b)
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          v1(row * numCols + col) += v2(row * numCols + col)
        }
      }
      v1
    })
    for (row <- 0 until numRows) {
      // log(p(C)) = b + sum exp(a_i - b) - log(N)
      for (col <- 0 until numCols) {
        sumExp(row * numCols + col) = maxLogPCOverX(row * numCols + col) + scala.math.log(sumExp(row * numCols + col)) - scala.math.log(numCols * numRows)
      }
    }
    sumExp
  }

  def getT(iteration: Int, maxIteration: Int): Double = {
    val T: Double = 1.0 * TMax * scala.math.pow(
      (TMin * 1.0) / TMax,
      iteration * 1.0 / (maxIteration - 1)
    )
    println("Mixed model: T = " + T)
    T
  }

  def itemsPerCell(logPCOverX: RDD[(Long, Array[Array[Double]])]): Array[Array[Int]] = {
    val itemsPerCell = logPCOverX.map((v: (Long, Array[Array[Double]])) => {
      val arr = v._2
      var maxVal = arr(0)(0)
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          if (arr(row)(col) > maxVal) {
            maxVal = arr(row)(col)
          }
        }
      }
      arr.map(_.map(v => {
        if (v == maxVal) 1 else 0
      }))
    })
      .reduce((v1: Array[Array[Int]], v2: Array[Array[Int]]) => {
        for (row <- 0 until numRows) {
          for (col <- 0 until numCols) {
            v1(row)(col) = v1(row)(col) + v2(row)(col)
          }
        }
        v1
      })
    itemsPerCell
  }

  def train(binData: RDD[(Long, Vector[Int])],
            contData: RDD[(Long, Vector[Double])],
            maxIteration: Int = 10
           ): Array[Array[Cell]] = {
    assert(maxIteration >= 2, "Max iteration must be bigger than 1")
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


      val logPXOverCStar: RDD[(Long, Array[Array[Double]])] = this.logPXOverCStar(logPXOverC, T)


      // compute p(x)
      val logPX: RDD[(Long, Double)] = this.logPX(cells, logPXOverCStar, T)

      val logPCAndCStarOverX: RDD[(Long, Array[Array[Double]])] = this.logPCAndCStarOverX(logPX, cells, logPXOverC, T)

      // compute p(c/x)
      val logPCOverX: RDD[(Long, Array[Double])] = this.logPCOverX(logPCAndCStarOverX)

      val t1 = logPCOverX.collect()

      // compute p(cStar/x)
      val logPCStarOverX: RDD[(Long, Array[Double])] = this.logPCStarOverX(logPCAndCStarOverX)

      val t2 = logPCStarOverX.collect()


      // compute p(c*) from p(c*/x)
      val logPCStar: Array[Double] = this.logPCStar(logPCStarOverX)




      // compute the mean for continuous data
      val contMean: Array[Array[Vector[Double]]] = this.continuousModel.mean(logPCOverX, contData)

      val a = "a"
      //
      //      // compute continuous standard deviation
      //      val contStd = this.continuousModel.std(logPCOverX, contData, contMean, contSize)
      //
      //      val binMean: Array[Array[DenseVector[Int]]] = this.binaryModel.mean(logPCOverX, binData)
      //
      //      val binEpsilon = this.binaryModel.std(logPCOverX, binMean, binData, binSize)
      //
      //      val numItemsPerCell: Array[Array[Int]] = itemsPerCell(logPCOverX)
      //
      //      for (row <- 0 until numRows) {
      //        for (col <- 0 until numCols) {
      //          cells(row)(col).contMean = contMean(row)(col)
      //          cells(row)(col).contStd = contStd(row)(col)
      //          cells(row)(col).binMean = binMean(row)(col)
      //          cells(row)(col).binEpsilon = binEpsilon(row)(col)
      //          cells(row)(col).prob = scala.math.exp(logPC(row)(col))
      //          cells(row)(col).numItems = numItemsPerCell(row)(col)
      //        }
      //      }

    }
    cells
  }
}
