// Big Data Application Development Project - Spring 2018
// Optimal Strategies for PlayerUnknown's Battlegrounds
// Mathew Thomas - N15690387
//
// Code to divide all data into subsets for easier processing
//
// Writes subsets of data to single csv using coalesce method
//

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._

val df1 = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/CleanedData/agg.csv")
val df2 = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/CleanedData/kills.csv")

val impCol = Seq("id","victim_position_x","victim_position_y","killed_by")

val erangel = df2.filter($"map" === "ERANGEL").select(impCol.head, impCol.tail: _*)
val miramar = df2.filter($"map" === "MIRAMAR").select(impCol.head, impCol.tail: _*)
erangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/ErangelDeaths")
miramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MiramarDeaths")

val erangel = df2.filter($"map" === "ERANGEL" && $"time"<800).select(impCol.head, impCol.tail: _*)
val miramar = df2.filter($"map" === "MIRAMAR" && $"time"<800).select(impCol.head, impCol.tail: _*)
erangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Erangel")
miramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Miramar")

val erangel = df2.filter($"map" === "ERANGEL" && $"time">=800 && $"time"<1600).select(impCol.head, impCol.tail: _*)
val miramar = df2.filter($"map" === "MIRAMAR" && $"time">=800 && $"time"<1600).select(impCol.head, impCol.tail: _*)
erangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Erangel")
miramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Miramar")

val erangel = df2.filter($"map" === "ERANGEL" && $"time">=1600).select(impCol.head, impCol.tail: _*)
val miramar = df2.filter($"map" === "MIRAMAR" && $"time">=1600).select(impCol.head, impCol.tail: _*)
erangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Erangel")
miramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Miramar")
