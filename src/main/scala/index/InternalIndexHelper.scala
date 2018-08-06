package index

import org.apache.spark.rdd.RDD


object InternalIndexHelper extends Serializable {
  /*
   * Euclidean Distance
   * Params:
   *  point1,point2: Array[Double] - 2 vectors that need to compute the distance
   * return: Double
   */
  def euclidean(point1: Array[Double], point2: Array[Double]): Double = {
    val sum = point1.zip(point2).map(v => scala.math.pow(v._1 - v._2, 2)).sum
    scala.math.sqrt(sum)
  }

  /*
  * Count the number of points in each cluster
  * Params:
  *  predict: RDD[String] - the RDD of all cluster labels
  * return: RDD[(String,Int)] - RDD of tuples of label and the corresponding amount
  */
  def clustersSize(predict: RDD[String]): RDD[(String, Int)] = {
    predict.map(v => (v, 1)).reduceByKey(_ + _)
  }

  /*
   * Scatter of point in cluster
   * Params:
   *  cluster: RDD[String] - the RDD of cluster that we need to compute
   * return: Double - Scatter value
   */
  def scatter(cluster: RDD[Array[Double]]): Double = {
    val centroid = center(cluster)
    val sumDistance = cluster.map(p => this.euclidean(centroid, p)).reduce(_ + _)
    sumDistance / cluster.count
  }

  /*
   * Centroid of cluster
   * Params:
   *  cluster: RDD[String] - the RDD of cluster that we need to compute
   * return: Array[Double] - The Centroid vector
   */
  def center(cluster: RDD[Array[Double]]): Array[Double] = {
    val dimen = cluster.take(1)(0).length
    val index = (0 until dimen).toArray
    val numPoints = cluster.count
    index.map(i => cluster.map(p => p(i).toDouble).reduce(_ + _) / numPoints)
  }

  /*
   * Measure of how good the clustering scheme is
   * Params:
   *  scatter1,scatter2: Double - the scatter value of cluster 1 and cluster 2
   *  center1,center2: Array[Double] - The centroid of cluster 1 and cluster 2
   */
  def good(scatter1: Double, scatter2: Double, center1: Array[Double], center2: Array[Double]): Double = {
    val m = this.euclidean(center1, center2)
    val num = scatter1 + scatter2
    num / m
  }

  /*
   * Compute the  within-cluster mean distance a(i) for all the point in cluster
   * Param: cluster: RDD[Array[Double]]
   * Return index of point and the corresponding a(i) Array[(Long, Double)]
   */
  def aiList(cluster: RDD[(Long, Array[Double])]): RDD[(Long, Double)] = {
    //pair each point with the others in clusters
    val pointPairs = cluster.cartesian(cluster).filter(v => v._1._1 != v._2._1)
    // Compute the distance between each pair
    val allPointsDistances = pointPairs.map(v => ((v._1._1, v._2._1), euclidean(v._1._2, v._2._2)))
    // Sum all the distances for each point
    val totalDistanceList = allPointsDistances.map(v => (v._1._1, v._2)).reduceByKey(_ + _)
    // Count the point in cluster
    val count = totalDistanceList.count
    // Compute the a{i} list
    val aiList = totalDistanceList.mapValues(_ / (count - 1))
    aiList
  }
}
