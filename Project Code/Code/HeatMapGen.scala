// Big Data Application Development Project - Spring 2018
// Optimal Strategies for PlayerUnknown's Battlegrounds
// Mathew Thomas - N15690387
//
// Code to generate cluster centers for a heat map based on PUBG data
//
// Generates cluster centers and weight of each cluster, save files locally
//

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import sqlContext._
import sqlContext.implicits._
import java.io._

val sqlContext = new SQLContext(sc)

def makeDoubleMiramar (s:org.apache.spark.sql.Row): Array[Double] = {
var str1 = s.toString
val toRemove = "[]".toSet
val words = str1.filterNot(toRemove)
var str = words.split(",")
return Array(str(0).toDouble*1000/800000,str(1).toDouble*1000/800000,str(2).toDouble*1000/800000)
}

def makeDoubleErangel (s:org.apache.spark.sql.Row): Array[Double] = {
var str1 = s.toString
val toRemove = "[]".toSet
val words = str1.filterNot(toRemove)
var str = words.split(",")
return Array(str(0).toDouble*4096/800000,str(1).toDouble*4096/800000,str(2).toDouble*4096/800000)
}

val numClusters = 50
val numIterations = 5

//----------------------------------------------------------------------------------------

//Early Game

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Miramar.csv")
val mrdd = miramar.rdd
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Erangel.csv")
val erdd = erangel.rdd

var mlocs = mrdd.map(m => makeDoubleMiramar(m))
var elocs = erdd.map(m => makeDoubleErangel(m))

val eparsedData = elocs.map(a => Vectors.dense(a(1),a(2))).cache()
val mparsedData = mlocs.map(a => Vectors.dense(a(1),a(2))).cache()

val eclusters = KMeans.train(eparsedData, numClusters, numIterations)
val mclusters = KMeans.train(mparsedData, numClusters, numIterations)

val WSSSE = eclusters.computeCost(eparsedData)
println("Erangel Within Set Sum of Squared Errors = " + WSSSE)

eclusters.clusterCenters.foreach(println)

val WSSSE = mclusters.computeCost(mparsedData)
println("Miramar Within Set Sum of Squared Errors = " + WSSSE)

mclusters.clusterCenters.foreach(println)

val ecluster_ind = eclusters.predict(eparsedData)
val total = ecluster_ind.count.toFloat
val eWeights = ecluster_ind.countByValue()
val emag = eWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val mcluster_ind = mclusters.predict(mparsedData)
val total = mcluster_ind.count.toFloat
val mWeights = mcluster_ind.countByValue()
val mmag = mWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val file = "erangelClusterCentersEarly.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- eclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "erangelClusterWeightsEarly.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- emag) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterCentersEarly.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterWeightsEarly.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mmag) {
  writer.write(x + "\n")
}
writer.close()

//----------------------------------------------------------------------------------------

//Mid Game

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Miramar.csv")
val mrdd = miramar.rdd
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Erangel.csv")
val erdd = erangel.rdd

var mlocs = mrdd.map(m => makeDoubleMiramar(m))
var elocs = erdd.map(m => makeDoubleErangel(m))

val eparsedData = elocs.map(a => Vectors.dense(a(1),a(2))).cache()
val mparsedData = mlocs.map(a => Vectors.dense(a(1),a(2))).cache()

val eclusters = KMeans.train(eparsedData, numClusters, numIterations)
val mclusters = KMeans.train(mparsedData, numClusters, numIterations)

val WSSSE = eclusters.computeCost(eparsedData)
println("Erangel Within Set Sum of Squared Errors = " + WSSSE)

eclusters.clusterCenters.foreach(println)

val WSSSE = mclusters.computeCost(mparsedData)
println("Miramar Within Set Sum of Squared Errors = " + WSSSE)

mclusters.clusterCenters.foreach(println)

val ecluster_ind = eclusters.predict(eparsedData)
val total = ecluster_ind.count.toFloat
val eWeights = ecluster_ind.countByValue()
val emag = eWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val mcluster_ind = mclusters.predict(mparsedData)
val total = mcluster_ind.count.toFloat
val mWeights = mcluster_ind.countByValue()
val mmag = mWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val file = "erangelClusterCentersMid.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- eclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "erangelClusterWeightsMid.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- emag) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterCentersMid.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterWeightsMid.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mmag) {
  writer.write(x + "\n")
}
writer.close()

//----------------------------------------------------------------------------------------

//Late Game

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Miramar.csv")
val mrdd = miramar.rdd
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Erangel.csv")
val erdd = erangel.rdd

var mlocs = mrdd.map(m => makeDoubleMiramar(m))
var elocs = erdd.map(m => makeDoubleErangel(m))

val eparsedData = elocs.map(a => Vectors.dense(a(1),a(2))).cache()
val mparsedData = mlocs.map(a => Vectors.dense(a(1),a(2))).cache()

val eclusters = KMeans.train(eparsedData, numClusters, numIterations)
val mclusters = KMeans.train(mparsedData, numClusters, numIterations)

val WSSSE = eclusters.computeCost(eparsedData)
println("Erangel Within Set Sum of Squared Errors = " + WSSSE)

eclusters.clusterCenters.foreach(println)

val WSSSE = mclusters.computeCost(mparsedData)
println("Miramar Within Set Sum of Squared Errors = " + WSSSE)

mclusters.clusterCenters.foreach(println)

val ecluster_ind = eclusters.predict(eparsedData)
val total = ecluster_ind.count.toFloat
val eWeights = ecluster_ind.countByValue()
val emag = eWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val mcluster_ind = mclusters.predict(mparsedData)
val total = mcluster_ind.count.toFloat
val mWeights = mcluster_ind.countByValue()
val mmag = mWeights.map{case (k,v) => (k,v.toFloat/total)}.toSeq.sortBy(_._1)

val file = "erangelClusterCentersLate.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- eclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "erangelClusterWeightsLate.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- emag) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterCentersLate.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mclusters.clusterCenters) {
  writer.write(x + "\n")
}
writer.close()

val file = "miramarClusterWeightsLate.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- mmag) {
  writer.write(x + "\n")
}
writer.close()
