 /*
 * Update by    : Chad Netwig
 * Project      : Hot Spot Analysis
 * Date         : June 29, 2023
 * Filename     : HotcellAnalysis.scala
 *              : 
 * Description  : Applied spatial statistics to spatio-temporal big data in order to identify
 *              : statistically significant spatial hot spots using Apache Spark.
 *              : 
 *              : In particular, implemented Getis-Ord statistic to produce a G-Score to list the fifty most significant
 *              : hot spot cells in time and space as identified using the Getis-ord statistic
 *              : 
 *              : Comments prepended with 'CLN' throughout detailing the specificis of the implementation
*/

package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

/*
  CLN: To calculate the Getis-Ord statistic and identify the fifty most significant hot spot cells, implemented the following steps in the code:

    1. Calculate the spatial weights for each pair of neighboring cells, using HotcellUtils.calcSpatialWeight
       Since the spatial weight is equal for each neighbor cell and equal to 1, we don't need to explicitly calculate the weights.

    2. Calculate the mean of the values for all cells

    3. Calculate the standard deviation of the values for all cells

    4. For each cell, calculate the Getis-Ord statistic using the formula:

       G_i = (sum of spatial weights * cell's hotness (num pickups) - mean * sum of spatial weights) / 
       (standard deviation * sqrt((numCells * sum of spatial weights^2) - (sum of spatial weights * sum of spatial weights) / n - 1))

    5. Sort the cells based on their Getis-Ord statistics in descending order

    6. Limited to top fifty cells by Entrance.scala

*/
  ///////////////////////////////
  // CLN: For debugging througout
  ///////////////////////////////

  val debugging = false // Set the value of the debugging flag


  if (debugging) {
    println(s"numCells: $numCells")                           // "s"" preceding string literal is string interpolation in scala
    println(s"pickupInfo total count: ${pickupInfo.count()}") // count = 100000
  }

  pickupInfo.createOrReplaceTempView("pickupInfoNormalizedCells")  // create view for normalized cells

 
  // CLN: For each unique combination of x, y, and z, perform a count and assign the count to a new column named cellHotness,
  //      which represents the count of pickup points for each unique cell combination of x, y, and z
  pickupInfo = spark.sql(
    "SELECT x, y, z, COUNT(*) as cellHotness " +
    "FROM pickupInfoNormalizedCells " +
    "GROUP BY x, y, z " +
    "ORDER BY cellHotness DESC"
  )
  pickupInfo.createOrReplaceTempView("cellHotnessCount")


  if (debugging) {

    println(s"cellHotnessCount total cell count: ${pickupInfo.count()}")
    println("cellHotnessCount:")      // = 3956 (cells)
    pickupInfo.show(false)
    
  }

  // CLN: Use 'selectExpr' dataFrame operator to calculate the sum of 'cellHotness' column, which is the sum of pickup points for all cells = 100000
  val sumOfPickupPoints = pickupInfo.selectExpr("sum(cellHotness)")
    .first()                          // Get the first row from the result
    .getLong(0)                       // Retrieve the value at column index 0 (quotient)

  if (debugging) { println("sumOfPickupPoints: " + sumOfPickupPoints) }

  // Calculate the mean = 0.9713358782333343
  val meanXbar = sumOfPickupPoints.toDouble / numCells  
 
  if (debugging) { println("Mean Xbar: " + meanXbar) }

  // Using selectExpr method to execute a SQL-like expression on the DataFrame to
  // calculate the sum of squared pickup points for all cells = 1.0211498E7 (10,211,498)
  val sumOfSquaredPickupPoints = pickupInfo.selectExpr("sum(pow(cellHotness, 2))").first().getDouble(0)
 
  if (debugging) { println("sumOfSquaredPickupPoints: " + sumOfSquaredPickupPoints) }

  // Calculate the standard deviation using scala sqrt =  9.911833856090206
  val stdDev = math.sqrt((sumOfSquaredPickupPoints / numCells.toDouble) - (meanXbar.toDouble * meanXbar.toDouble))
 
  if (debugging) { println("Standard Deviation: " + stdDev) }

  // Register the user-defined function that is a method of HotcellUtils
  spark.udf.register("cellSpatialWeight", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => 
    HotcellUtils.calcSpatialWeight(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))
  

  // Create a new column (using withColumn) with the spatial weight for each respective cell
  val cellSpatialWeightTotal = pickupInfo.withColumn("cellSpatialWeightTotal",
    expr(s"cellSpatialWeight(x, y, z, $minX, $maxX, $minY, $maxY, $minZ, $maxZ)")
  )
  
  cellSpatialWeightTotal.createOrReplaceTempView("cellSpatialWeightTotal")

  if (debugging) {

    println("cellSpatialWeightTotal:")
    cellSpatialWeightTotal.show()

  }

  // TODO: Implement Getis-Ord in same way as cellSpatialWeightTotal
	
  // Joining cellSpatialWeightTotal with itself based on proximity of coordinates in order to aggregate a cells hotness with respect
  // its adjacent cell's hotness
  val cellSpatialWeightTotalWithAggHotCells = cellSpatialWeightTotal.alias("currCells").join(cellSpatialWeightTotal.alias("adjCells"),

        (col("adjCells.x") === col("currCells.x") + 1 || col("adjCells.x") === col("currCells.x") || col("adjCells.x") === col("currCells.x") - 1) &&
        (col("adjCells.y") === col("currCells.y") + 1 || col("adjCells.y") === col("currCells.y") || col("adjCells.y") === col("currCells.y") - 1) &&
        (col("adjCells.z") === col("currCells.z") + 1 || col("adjCells.z") === col("currCells.z") || col("adjCells.z") === col("currCells.z") - 1)

      )
      // Grouping by coordinates and aggregating the data:
      // The aggHotCells column represents the total sum of the "cellHotness" values from the adjacent cells for each group of current cells,
      // providing an aggregated measure of the hotness of the neighboring cells.
      .groupBy("currCells.z", "currCells.y", "currCells.x")
      .agg(sum(when(col("adjCells.cellHotness").isNotNull, col("adjCells.cellHotness"))).alias("aggHotCells"),
      
        // add the cellHotness and cellSpatialWeightTotal columns back into the DataFrame after the aggregation
        first("currCells.cellHotness").alias("cellHotness"),
        first("currCells.cellSpatialWeightTotal").alias("cellSpatialWeightTotal")
      )

      //.orderBy("currCells.z", "currCells.y", "currCells.x")
  cellSpatialWeightTotalWithAggHotCells.createOrReplaceTempView("cellSpatialWeightTotalWithAggHotCells")

  if (debugging) {

    println("cellSpatialWeightTotalWithAggHotCells:")
    cellSpatialWeightTotalWithAggHotCells.show()

  }

  /////////////////////////////////////////
  // CLN: Calculate the Getis-Ord statistic
  /////////////////////////////////////////
  val getisOrd = spark.sql(
   
    s"SELECT " +
      // numerator
      s"(SUM(aggHotCells) - " +
      s"(SUM(cellSpatialWeightTotal) * $meanXbar)) / " +
      // denominator
      s"($stdDev * SQRT(((${numCells.toDouble} * " +
      s"(SUM(cellSpatialWeightTotal))) - " +
      s"(SUM(cellSpatialWeightTotal) * SUM(cellSpatialWeightTotal))) / " +
      s"(${numCells.toDouble} - 1))) AS getisOrdStatistic, " +
      "x, y, z " +
    "FROM " +
      "cellSpatialWeightTotalWithAggHotCells " +
    "GROUP BY " +
      "x, y, z " +
    "ORDER BY " +
      "getisOrdStatistic DESC"

  )
  getisOrd.createOrReplaceTempView("getisOrd")

  if (debugging) {

    println("getisOrd:")
    getisOrd.show()

  }

  // Return a new DataFrame with only the columns x, y, and z (no G-Score)
  val hotCells = getisOrd.select("x", "y", "z")

  if (debugging) { 

    println("hotCells:")  
    hotCells.show()

  }
  
  return hotCells

  //return pickupInfo // YOU NEED TO CHANGE THIS PART

  } // close runHotcellAnalysis method

} //close HotcellAnalysis object
