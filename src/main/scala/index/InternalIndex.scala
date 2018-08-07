package index

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class InternalIndex(var point: RDD[Array[Double]], var predict: RDD[String], context: SparkContext) {
  if (predict.count != point.count) {
    throw new Exception("the length of predict and points must be the same")
  }
  private val clusterLabels: RDD[String] = predict.distinct

  //zip the predict label with the point
  private val combinedRDD: RDD[(String, Array[Double])] = predict.zip(point)
  private val sc: SparkContext = context

  def scattersList(): RDD[(String, Double)] = {
    val clusters = this.clusterLabels.collect.map(l => (l, this.combinedRDD.filter(v => v._1 == l).map(_._2)))
    val scatters = this.sc.parallelize(clusters.map(x => (x._1, InternalIndexHelper.scatter(x._2))))
    scatters
  }

  def centersList(): RDD[(String, Array[Double])] = {
    val clusters = this.clusterLabels.collect.map(l => (l, this.combinedRDD.filter(v => v._1 == l).map(_._2)))
    val centers = this.sc.parallelize(clusters.map(x => (x._1, InternalIndexHelper.center(x._2))))
    centers
  }


  /*
   * Davies Bouldin Index
   */
  def davies_bouldin(): Double = {
    var scatters = this.scattersList()
    val centers = this.centersList()
    val clustersWithCenterandScatters = scatters.join(centers)
    val cart = clustersWithCenterandScatters.cartesian(clustersWithCenterandScatters).filter(v => v._1._1 != v._2._1)
    val rijList = cart.map(v => ((v._1._1, v._2._1), InternalIndexHelper.good(v._1._2._1, v._2._2._1, v._1._2._2, v._2._2._2)))
    val di = rijList.map(v => (v._1._1, v._2)).reduceByKey((a, b) => scala.math.max(a, b))
    val numCluster = clusterLabels.count
    val davies_bouldin = di.map(v => v._2).reduce(_ + _) / numCluster
    davies_bouldin
  }

  /*
   * The mean of the silhouette widths for a given cluster
   * Param: label: String - the cluster label that we want to compute
   * Return Double
   */
  def sk(label: String): Double = {
    // index all point
    val indexedData = this.combinedRDD.zipWithIndex().map(v => (v._2, v._1))
    // get the target cluster
    val target = indexedData.filter(x => x._2._1 == label)
    // get others cluster
    val others = indexedData.filter(x => x._2._1 != label)
    // catersian the target with others
    val cart = target.cartesian(others)
    //get the sum distance between each point and other clusters
    val allDistances = cart.map(x => ((x._1._1, x._2._2._1), InternalIndexHelper.euclidean(x._1._2._2, x._2._2._2))).reduceByKey(_ + _)
    // numbers of point of others clusters
    val numPoints = others.map(x => (x._2._1, 1)).reduceByKey(_ + _).collectAsMap
    //mean distance of point to the points of the other clusters
    val deltas = allDistances.map(x => (x._1._1, x._2 / numPoints.getOrElse(x._1._2, 1)))
    // Compute b(i) the smallest of these mean distances
    val bi = deltas.reduceByKey((a, b) => if (a > b) b else a)
    val ai = InternalIndexHelper.aiList(target.map(x => (x._1, x._2._2)))
    val si = ai.join(bi).map(x => (x._2._2 - x._2._1) / scala.math.max(x._2._2, x._2._1))
//    val test1 = si.collect()
//    val test2 = si.count()
    if (si.count == 0) {
      return 0
    }
    val sk = si.sum / si.count
    sk
  }

  /*
   * Silhouette Index
   */
  def silhouette(): Double = {
    val num = this.clusterLabels.collect.map(sk)
    val denom = this.clusterLabels.count()
    this.clusterLabels.collect.map(sk).sum / this.clusterLabels.count
  }
}