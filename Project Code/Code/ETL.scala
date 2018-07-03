// Big Data Application Development Project - Spring 2018
// Optimal Strategies for PlayerUnknown's Battlegrounds
// Mathew Thomas - N15690387
//
// Code to clean and profile PUBG data
//
// Writes clean data to single csv using coalesce method
//

//Load Data
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._

val df1 = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/agg.csv")
val df2 = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/kills.csv")

//Profiling Function
def profiling( df: org.apache.spark.sql.DataFrame ) : scala.collection.mutable.Map[String,(Any,Any)] = {
      val numCol = df.columns.length
      val types = df.dtypes
      val map = scala.collection.mutable.Map[String,(Any,Any)]()
      for( i <- 0 until (numCol-1)){
         val selectedColumnName = df.columns(i)
         if( types(i)._2 == "StringType" ){
            val minMax = Array(df.agg(min(length(df(selectedColumnName)))).as[Int].first, df.agg(max(length(df(selectedColumnName)))).as[Int].first)
            map(selectedColumnName) = (minMax(0),minMax(1))
         }
         else{
            val minMax = df.agg(min(selectedColumnName), max(selectedColumnName)).collect
            map(selectedColumnName) = (minMax(0)(0),minMax(0)(1))
         }
      }
      return map
}

//Profile both datasets
profiling(df1)
profiling(df2)

//Show number of null values in each dataset
df1.select(df1.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show
df2.select(df2.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show

//Replace Null Values with suitable replacements, assigning to a new dataframe
val map = Map("killer_placement" -> 0.0, "killer_position_x" -> -999999.0, "killer_position_y" -> -999999.0, "victim_placement" -> 0.0)
val df2_clean = df2.na.fill(map)

val df2 = df2_clean.withColumn("id", monotonicallyIncreasingId)
val df1_update = df1.withColumn("id", monotonicallyIncreasingId)
val df1 = df1_update

//Check that all null values have been removed
df2_clean.select(df2_clean.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show

df1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/CleanedData/agg.csv")

df2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/CleanedData/kills.csv")

