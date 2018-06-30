package com.quan.model

import breeze.linalg._
import com.quan.util.DistributionHelper
import org.apache.spark.rdd.RDD

class MixedModel(numRows: Int, numCols: Int, TMin: Int = 1, TMax: Int = 10) extends Serializable {

  val binaryModel = new BinaryModel(numRows, numCols)
  val continuousModel = new ContinuousModel(numRows, numCols)
  /**
    *
    * @return
    */
  def createCells(contSize: Int, binSize: Int): Array[Array[Cell]] = {
    val prob = 1.0 / (numCols * numRows)
    val temp = for (row <- 0 to numRows)
      yield (
        for (col <- 0 to numCols)
          yield new Cell(row, col, contSize, binSize, prob)
        ).toArray
    temp.toArray
  }

  def pXOverC(binData: RDD[(Long, Vector[Int])], contData: RDD[(Long, Vector[Double])], cells: Array[Array[Cell]]): RDD[(Long, Array[Array[Double]])] = {
    // compute the bernouli of x over c
    val pXBinOverC: RDD[(Long, Array[Array[Double]])] = this.binaryModel.pXOverC(binData, cells)

    // compute gaussian
    val pXContOverC: RDD[(Long, Array[Array[Double]])] = this.continuousModel.pXOverC(contData, cells)

    // compute the p(x/c)
    val pXOverC = pXBinOverC.join(pXContOverC).map((p: (Long, (Array[Array[Double]], Array[Array[Double]]))) => {
      val temp = for (row <- 0 until numRows)
        yield (
          for (col <- 0 until numCols)
            yield p._2._1(row)(col) * p._2._2(row)(col)
          ).toArray
      (p._1, temp.toArray)
    })

    pXOverC
  }

  // compute p(c/c*)
  def pCOverCStar(c: (Int, Int), cStar: (Int, Int), T: Double): Double = {
    var sum: Double = 0.0
    for (row <- 0 until numRows) {
      for (col <- 0 until numCols) {
        val r = (row, col)
        sum = sum + DistributionHelper.kernel(DistributionHelper.distance(cStar, r), T)
      }
    }
    DistributionHelper.kernel(DistributionHelper.distance(c, cStar), T) / sum
  }

  // compute p(x)
  def pX(cells: Array[Array[Cell]], pXOverC: RDD[(Long, Array[Array[Double]])], T: Double): RDD[(Long, Double)] = {
    pXOverC.mapValues(v => {
      var pX: Double = 0.0
      for (rowStar <- 0 until numRows) {
        for (colStar <- 0 until numCols) {

          // get c*
          val cStar = (rowStar, colStar)

          // p(c*)
          val pCStar = cells(rowStar)(colStar).prob

          // p(x/c*)
          var pXOverCStar: Double = 0.0

          for (row <- 0 until numRows) {
            for (col <- 0 until numCols) {
              // get c
              val c = (row, col)

              // p(x/c)
              val pXOverCValue = v(row)(col)

              // p(c/c*)
              val pCOverCStar = this.pCOverCStar(c, cStar, T)

              // p(x/c*) = sum p(x / c) * p(c/c*)
              pXOverCStar += pXOverCValue * pCOverCStar
            }
          }
          // p(x) = p(c*) x p(x/c*)
          pX += pCStar * pXOverCStar
        }
      }
      pX
    })
  }

  // compute p(c/x)
  def pCOverX(pX: RDD[(Long, Double)],
              pXOverC: RDD[(Long, Array[Array[Double]])],
              cells: Array[Array[Cell]],
              T: Double): RDD[(Long, Array[Array[Double]])] = {
    pX.join(pXOverC).map(v => {
      val px: Double = v._2._1
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
              val pCAndCStar: Double = (pCOverCStar * pCStar * pxOverC(row)(col)) / px

              pCOverXArr(row)(col) += pCAndCStar

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
    * @param pCOverX
    * @return
    */
  def pC(pCOverX: RDD[(Long, Array[Array[Double]])]): Array[Array[Double]] = {
    val t = pCOverX.map(_._2).reduce((v1, v2) => {
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
    TMax *
      scala.math.pow(
        TMin / TMax,
        iteration / maxIteration
      )
  }

  def train(binData: RDD[(Long, Vector[Int])],
            contData: RDD[(Long, Vector[Double])],
            maxIteration: Int = 10
           ) = {
    var iteration: Int = 0

    val contSize = contData.take(1)(0)._2.size
    val binSize = binData.take(1)(0)._2.size

    var cells: Array[Array[Cell]] =
      createCells(contSize, binSize)


    while (iteration < maxIteration) {
      iteration += 1

      val T: Double = getT(iteration, maxIteration)

      // compute p(x/c)
      val pXOverC: RDD[(Long, Array[Array[Double]])] = this.pXOverC(binData, contData, cells)

//      val test = pXOverC.take(2)


      // compute p(x)
      val pX: RDD[(Long, Double)] = this.pX(cells, pXOverC, T)

      // compute p(c/x)
      val pCOverX: RDD[(Long, Array[Array[Double]])] = this.pCOverX(pX, pXOverC, cells, T)

      // compute p(c) from p(c/x)
      val pC: Array[Array[Double]] = this.pC(pCOverX)

      // compute the mean for continuous data
      val contMean: Array[Array[Vector[Double]]] = this.continuousModel.mean(pCOverX, contData)

      // compute continuous standard deviation
      val contStd = this.continuousModel.std(pCOverX, contData, contMean, contSize)


      val binMean: Array[Array[DenseVector[Int]]] = this.binaryModel.mean(pCOverX, binData)

      val binStd = this.binaryModel.std(pCOverX, binMean, binData, binSize)

      //

    }
  }
}
