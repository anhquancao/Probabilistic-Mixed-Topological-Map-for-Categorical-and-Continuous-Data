package index

import org.apache.spark.rdd.RDD

/*
 * rdd1: the real cluster label
 * rdd2: the predicted cluster label
 *
 */
class ExternalIndex(var rdd1: RDD[String], var rdd2: RDD[String]) extends Serializable {

  private val combinedRDD: RDD[(String, String)] = rdd1.zip(rdd2)
  //   combinedRDD.cache

  // Cartesian the combinedRDD with itself so we can use the map function to process on the pair of points
  private val cart: RDD[((String, String), (String, String))] = combinedRDD.cartesian(combinedRDD)

  /*
   * Take in 2 pair of cluster label and return the yy,ny,nn,yn
   */
  private def notation(point1: (String, String), point2: (String, String)) = {
    if ((point1._1 == point2._1) && (point1._2 == point2._2)) {
      ("yy", 1)
    } else if ((point1._1 == point2._1) && (point1._2 != point2._2)) {
      ("yn", 1)
    } else if ((point1._1 != point2._1) && (point1._2 == point2._2)) {
      ("ny", 1)
    } else {
      ("nn", 1)
    }
  }

  /*
   * Compute the confusion table
   */
  def confusionTable(): RDD[((String, String), Int)] = {
    this.combinedRDD.map(line => (line, 1)).reduceByKey(_ + _)
  }

  def getNotations(): RDD[(String, Int)] = {
    val resultNotation = this.cart.map(v => this.notation(v._1, v._2))
    resultNotation.reduceByKey(_ + _)
  }

  def purity(): Double = {
    val table = this.confusionTable()
    val cols = table.map(v => (v._1._2, v))
    val t = cols.map(v => (v._1, v._2._2))

    val sum = t.reduceByKey(_ + _)
    val max = t.reduceByKey((a, b) => if (a > b) {
      a
    } else {
      b
    })

    sum.join(max).map(v => v._2).map(a => a._2.toDouble / a._1).reduce(_ + _) / sum.count
  }

  def nmi(): Double = {
    val num_rows = combinedRDD.count()
    val t = this.confusionTable()

    val cols = t.map(v => (v._1._2, v._2)).reduceByKey(_ + _).collectAsMap
    val rows = t.map(v => (v._1._1, v._2)).reduceByKey(_ + _).collectAsMap
    println(cols)
    val rightDenum = t.map(v => (v._1._2, v._2)).reduceByKey(_ + _).map(v => cols(v._1) * scala.math.log10(v._2.toDouble / num_rows)).reduce(_ + _)
    val leftDenum = t.map(v => (v._1._1, v._2)).reduceByKey(_ + _).map(v => rows(v._1) * scala.math.log10(v._2.toDouble / num_rows)).reduce(_ + _)

    val numerator = -2 * t.map(v => scala.math.log10((v._2 * num_rows).toDouble / (rows(v._1._1) * cols(v._1._2))) * v._2).reduce(_ + _)

    numerator / (leftDenum + rightDenum)
  }

  def precision(): Double = this.yy() / (this.yy() + this.ny())

  def recall(): Double = this.yy() / (this.yy() + this.yn())


  def yy(): Double = {
    val notations = this.getNotations()
    if (notations.lookup("yy").isEmpty) {
      0.0
    } else {
      ((notations.lookup("yy")(0) - combinedRDD.count()) / 2).toDouble
    }
  }

  def yn(): Double = {
    val notations = this.getNotations()
    if (notations.lookup("yn").isEmpty) {
      0.0
    } else {
      (notations.lookup("yn").head / 2).toDouble
    }

  }

  def ny(): Double = {
    val notations = this.getNotations()
    if (notations.lookup("ny").isEmpty) {
      0.0
    } else {
      (notations.lookup("ny").head / 2).toDouble
    }
  }

  def nn(): Double = {
    val notations = this.getNotations()
    if (notations.lookup("nn").isEmpty) {
      0.0
    } else {
      (notations.lookup("nn").head / 2).toDouble
    }
  }

  def nt(): Double = {
    this.yy + this.yn + this.nn + this.ny
  }

  def czekanowskiDice(): Double = (2 * this.yy) / (2 * this.yy + this.yn + this.ny)

  def rand(): Double = (this.yy + this.nn) / this.nt

  def rogersTanimoto(): Double = {
    val denominator = this.yy + (2 * (this.yn + this.ny)) + this.nn
    (this.yy + this.nn) / denominator
  }

  def folkesMallows(): Double = {
    val denominator = (this.yy + this.yn) * (this.ny + this.yy)
    this.yy / scala.math.sqrt(denominator)
  }

  //   def hubert():Double = {
  //     val yy_yn = this.yy + this.yn
  //     val yy_ny = this.yy + this.ny
  //     val nn_yn = this.nn + this.yn
  //     val nn_ny = this.nn + this.ny
  //     val denominator = math.sqrt(yy_yn) * math.sqrt(yy_ny) * math.sqrt(nn_yn) * math.sqrt(nn_ny)
  //     ((this.nt*this.yy) - (yy_yn*yy_ny))/denominator
  //   }
  def jaccard(): Double = this.yy / (this.yy + this.yn + this.ny)

  def kulczynski(): Double = ((this.yy / (this.yy + this.ny)) + (this.yy / (this.yy + this.yn))) / 2

  def mcNemar(): Double = (this.nn - this.ny) / scala.math.sqrt(this.nn + this.ny)

  //   def phi():Double = {
  //     val num = (this.yy*this.nn)-(this.yn*this.ny)
  //     val denum = (this.yy + this.yn)*(this.yy + this.ny)*(this.yn + this.nn)*(this.ny + this.nn)
  //     num/denum
  //   }
  def russelRao(): Double = this.yy / this.nt

  def sokalSneath1(): Double = {
    val denum = this.yy + (2 * (this.yn + this.ny))
    this.yy / denum
  }

  def sokalSneath2(): Double = {
    val denum = this.yy + this.nn + ((yn + ny) / 2)
    (this.yy + this.nn) / denum
  }
}