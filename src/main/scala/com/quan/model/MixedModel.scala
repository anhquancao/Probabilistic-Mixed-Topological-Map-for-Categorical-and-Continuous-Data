package com.quan.model

import java.nio.file.{Files, Path, Paths}
import java.util.Calendar

import breeze.linalg._
import com.quan.main.Reader
import com.quan.util.{DistributionHelper, IOHelper, RandomHelper}
import org.apache.spark.rdd.RDD

class MixedModel(numRows: Int, numCols: Int, TMin: Int = 1, TMax: Int = 10) extends Serializable {

  private val binaryModel = new BinaryModel(numRows, numCols)
  private val continuousModel = new ContinuousModel(numRows, numCols)

  var dirName: String = "out/exp " + Calendar.getInstance().getTime

  private var logPXOverC: RDD[(Long, Array[Array[Double]])] = _

  private var logPXOverCStar: RDD[(Long, Array[Array[Double]])] = _

  private var logPX: RDD[(Long, Double)] = _

  private var logPCAndCStarOverX: RDD[(Long, Array[Array[Double]])] = _

  private var logPCOverX: RDD[(Long, Array[Double])] = _

  private var logPCStarOverX: RDD[(Long, Array[Double])] = _

  private var logPCStar: Array[Double] = _

  private var contMean: Array[Array[Vector[Double]]] = _

  private var contVariance: Array[Array[Double]] = _

  private var binMean: Array[Array[DenseVector[Int]]] = _


  private var binEpsilon: Array[Array[Double]] = _

  private var numItemsPerCell: Array[Array[Double]] = _

  private var labels: RDD[(Long, Int)] = _

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
    val temp = for (row <- 0 until numRows)
      yield (
        for (col <- 0 until numCols)
          yield new Cell(row, col, contSize, binSize, prob, contMean, binMean)
        ).toArray
    temp.toArray
  }

  def loadCells(contSize: Int, binSize: Int): Array[Array[Cell]] = {

    println("Mixed mode: Load cells")

    val folder = "./out/exp Wed Aug 29 10:23:39 CEST 2018/"

    //    var contMean: Vector[Double] = contData.map(_._2).reduce((v1, v2) => v1 + v2).map(_ / contSize)
    var contMean: Array[DenseVector[Double]] = Reader
      .read(folder + "contCentroid100", ",")
      .map((arr: Array[String]) => new DenseVector[Double](arr.map(_.toDouble)))
      .collect()

    var probs: Array[Array[Double]] = Reader
      .read(folder + "prob-100", ",")
      .map(_.map(_.toDouble))
      .collect()


    var binMean: Vector[Int] = RandomHelper.createRandomBinaryVector(binSize)

    val prob = 1.0 / (numCols * numRows)
    val temp = for (row <- 0 until numRows)
      yield (
        for (col <- 0 until numCols)
          yield new Cell(row, col, contSize, binSize, probs(row)(col), contMean(row * numCols + col), binMean)
        ).toArray
    temp.toArray
  }

  def logPXOverC(binData: RDD[(Long, Vector[Int])], contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {

    println("Mixed model: Compute pXOverC")

    // compute the bernouli of x over c
    val logPXBinOverC: RDD[(Long, Array[Array[Double]])] = this.binaryModel.logPXOverC(binData, cells)

    // compute gaussian
    val logPXContOverC: RDD[(Long, Array[Array[Double]])] = this.continuousModel.logPXOverC(contData, cells)
    val p = logPXContOverC.take(20)

    val b = logPXBinOverC.take(3)


    // compute the p(x/c)
    val logPXOverC = logPXBinOverC.join(logPXContOverC).map((p: (Long, (Array[Array[Double]], Array[Array[Double]]))) => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
          //            yield p._2._1(row)(col) + p._2._2(row)(col)
            yield p._2._2(row)(col)
          ).toArray
      (p._1, temp.toArray)
    })

    //    val test = logPXOverC.collect()

    logPXOverC
  }

  //  var count = 0
  // compute p(c/c*)
  def pCOverCStar(c: (Int, Int), cStar: (Int, Int), T: Double): Double = {
    //    count += 1
    //    println("Mixed model: Compute pCOverCStar " + count)
    var pCOverCStarSum = 0.0
    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        val r = (row, col)
        pCOverCStarSum = pCOverCStarSum + DistributionHelper.kernel(DistributionHelper.distance(cStar, r), T)
      }
    }

    DistributionHelper.kernel(DistributionHelper.distance(c, cStar), T) / pCOverCStarSum
  }

  // compute p(x/c*)
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

              val logPCOverCStar: Double = scala.math.log(this.pCOverCStar(c, cStar, T))

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
    logPXOverCStar.mapValues((logPXOverCStar: Array[Array[Double]]) => {
      val logArr: Array[Array[Double]] = (
        for (row <- 0 until numRows)
          yield (
            for (col <- 0 until numCols)
              yield logPXOverCStar(row)(col) + scala.math.log(cells(row)(col).prob))
            .toArray
        ).toArray

      val maxValue: Double = logArr.map(_.max).max

      val exp = logArr.map(_.map(a => scala.math.exp(a - maxValue)))
      val logExpSum = scala.math.log(exp.map(_.sum).sum)
      maxValue + logExpSum
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
      val maxLogCStar: Array[Double] = logPCAndCStarOverX.map(_.max)

      val N = numRows * numCols
      val expSum: Array[Double] = (for (i <- 0 until N) yield 0.0).toArray

      for (cStar <- 0 until N) {
        for (c <- 0 until N) {
          expSum(cStar) += scala.math.exp(logPCAndCStarOverX(cStar)(c) - maxLogCStar(cStar))
        }
      }

      val logExpSum: Array[Double] = expSum.map(scala.math.log)
      for (cStar <- 0 until N) {
        logExpSum(cStar) = maxLogCStar(cStar) + logExpSum(cStar)
      }
      logExpSum
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

      val expSum: Array[Array[Double]] = RandomHelper.create2dArray(numCells, numCells, 0.0)
      for (cStar <- 0 until numCells) {
        for (c <- 0 until numCells) {
          expSum(cStar)(c) = scala.math.exp(logPCAndCStarOverX(cStar)(c) - maxLogC(c))
        }
      }

      //      val expSum: Array[Array[Double]] = logPCAndCStarOverX
      //        .map(
      //          _.zipWithIndex
      //            .map { case (v, i) => scala.math.exp(v - maxLogC(i)) }
      //        )

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
    val dataSize = logPCStarOverX.count()
    val maxLogPCOverX: Array[Double] = logPCStarOverX.map(_._2).reduce((v1: Array[Double], v2: Array[Double]) => {
      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          if (v1(row * numCols + col) < v2(row * numCols + col))
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
        sumExp(row * numCols + col) = maxLogPCOverX(row * numCols + col) + scala.math.log(sumExp(row * numCols + col)) - scala.math.log(dataSize)
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

  def assignItemsToCluster(logPCStarOverX: RDD[(Long, Array[Double])]): RDD[(Long, Int)] = {
    logPCStarOverX.map((v: (Long, Array[Double])) => {
      (v._1, v._2.zipWithIndex.maxBy(_._1)._2)
    })
  }

  def itemsPerCell(logPCStarOverX: RDD[(Long, Array[Double])]): Array[Array[Double]] = {
    val itemsPerCell: Array[Int] = logPCStarOverX.map((v: (Long, Array[Double])) => {
      val arr = v._2
      val maxVal = arr.max
      arr.map(v => {
        if (v == maxVal) 1 else 0
      })
    })
      .reduce((v1: Array[Int], v2: Array[Int]) => {
        for (i <- 0 until numRows * numCols) {
          v1(i) = v1(i) + v2(i)
        }
        v1
      })

    val res: Array[Array[Double]] = RandomHelper.create2dArray(numRows, numCols, 0.0)
    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        res(row)(col) = itemsPerCell(DistributionHelper.index(row, col, numCols))
      }
    }
    res
  }

  def predictedTrainingLabels: RDD[(Long, Int)] = {
    labels
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


    val path: Path = Paths.get(dirName)
    Files.createDirectories(path)

    while (iteration < maxIteration) {
      iteration += 1

      println("Iteration: " + iteration)

      val T: Double = getT(iteration, maxIteration)

      // compute p(x/c)
      // checked
      logPXOverC = this.logPXOverC(binData, contData, cells)

      //      val logPXOverCCollect = logPXOverC.collect()

      // compute p(x/c*)
      // checked
      logPXOverCStar = this.logPXOverCStar(logPXOverC, T)

      //      val logPXOverCStarCollect = logPXOverCStar.collect()

      // compute p(x)
      // checked
      logPX = this.logPX(cells, logPXOverCStar, T)

      //      val logPXCollect: Array[(Long, Double)] = logPX.collect()

      // compute p(c,c*/x)
      // checked
      logPCAndCStarOverX = this.logPCAndCStarOverX(logPX, cells, logPXOverC, T)

      //      val logPCAndCStarOverXCollect = logPCAndCStarOverX.collect()

      // compute p(c/x)
      // checked
      logPCOverX = this.logPCOverX(logPCAndCStarOverX)

      //      val logPCOverXCollect = logPCOverX.collect()


      // compute p(cStar/x)
      // checked
      logPCStarOverX = this.logPCStarOverX(logPCAndCStarOverX)

      //      val logPCStarOverXCollect = logPCStarOverX.collect()

      // compute p(c*) from p(c*/x)
      // checked
      logPCStar = this.logPCStar(logPCStarOverX)

      //      val contDataCollect = contData.collect()


      // compute the mean for continuous data
      // checked
      contMean = this.continuousModel.mean(logPCOverX, contData)

      // compute continuous variance
      // checked
      contVariance = this.continuousModel.variance(logPCOverX, contData, contMean, contSize)

      //      val binDataCollect = binData.collect()

      // checked
      binMean = this.binaryModel.mean(logPCOverX, binData)


      // checked
      binEpsilon = this.binaryModel.epsilon(logPCOverX, binMean, binData, binSize)

      //      val a = "a"

      numItemsPerCell = itemsPerCell(logPCStarOverX)

      labels = assignItemsToCluster(logPCStarOverX)

      for (row <- 0 until numRows) {
        for (col <- 0 until numCols) {
          cells(row)(col).contMean = contMean(row)(col)
          cells(row)(col).contVariance = contVariance(row)(col)
          cells(row)(col).binMean = binMean(row)(col)
          cells(row)(col).binEpsilon = binEpsilon(row)(col)
          cells(row)(col).prob = scala.math.exp(logPCStar(DistributionHelper.index(row, col, numCols)))
          cells(row)(col).numItems = numItemsPerCell(row)(col)
        }
      }
      IOHelper.writeCells(iteration, numRows, numCols, cells, dirName)
      val dataWithLabels: Array[(Long, (Vector[Double], Int))] = contData.join(labels).collect()
      IOHelper.writeLabels(iteration, dataWithLabels, dirName)
    }
    cells
  }
}
