// Big Data Application Development Project - Spring 2018
// Optimal Strategies for PlayerUnknown's Battlegrounds
// Mathew Thomas - N15690387
//
// Code to generate best loadouts for different stages of the game
//
// Generates ranking of best loadouts for different stages of the game, and
// saves each ranking to a single csv using the coalesce method
//

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Miramar.csv")
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/EarlyGame/Erangel.csv")

val causeOfDeathErangel =  erangel.groupBy("killed_by").count().sort(desc("count"))
val causeOfDeathMiramar =  miramar.groupBy("killed_by").count().sort(desc("count"))

causeOfDeathErangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/EarlyGame/Erangel")
causeOfDeathMiramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/EarlyGame/Miramar")

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Miramar.csv")
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/MidGame/Erangel.csv")

val causeOfDeathErangel =  erangel.groupBy("killed_by").count().sort(desc("count"))
val causeOfDeathMiramar =  miramar.groupBy("killed_by").count().sort(desc("count"))

causeOfDeathErangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/MidGame/Erangel")
causeOfDeathMiramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/MidGame/Miramar")

val miramar = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Miramar.csv")
val erangel = sqlContext.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs:/user/mt3443/BDAD/ProjectData/DerivedData/LateGame/Erangel.csv")

val causeOfDeathErangel =  erangel.groupBy("killed_by").count().sort(desc("count"))
val causeOfDeathMiramar =  miramar.groupBy("killed_by").count().sort(desc("count"))

causeOfDeathErangel.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/LateGame/Erangel")
causeOfDeathMiramar.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/mt3443/BDAD/ProjectData/Results/Loadouts/LateGame/Miramar")
