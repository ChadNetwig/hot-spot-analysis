package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  // pointPath and rectanglePath load in the /src/resources/point-hotzone.csv and /src/resources/zone-hotzone.csv, respectively
  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats (CLN: this is from point-hotzone.csv)
    // CLN: trim removes parentheses from the string so will be compatible on joining the two datasets below
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")

    // Load rectangle data (CLN: this is from zone-hotzone.csv)
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")

    // Join two datasets
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")

    // YOU NEED TO CHANGE THIS PART

    // CLN: For debugging
    // joinDf.show(false)

    val debugging = false // Set the value of the debugging flag

    if (debugging) {
      println("\npointDF after trimming parens and parsing point-hotzone.csv (col 5 only): ")
      pointDf.show(false)
    
      println("rectangleDF after parsing zone-hotzone.csv (all cols): ")
      rectangleDf.show(false)

      println("Displaying joinResult tempview:")
      spark.sql("SELECT * FROM joinResult").show(false)

    }

    // CLN: Create hotzone Data Frame from rectangle and point columns in joinResult view (range join query above), calculate the points
    //      inside the rectangles to determine "hotness", group  by rectangle, and sort in rectangle in ascending order (according to hotness)    
    val hotzoneDf = spark.sql("SELECT rectangle, COUNT(point) as numPoints FROM joinResult GROUP BY rectangle ORDER BY rectangle ASC")
    hotzoneDf.createOrReplaceTempView("hotZones")

    // collapse hotzoneDf dataframe down to a single partition for return
    val coalesceHotzoneDf = hotzoneDf.coalesce(1)

    if (debugging) {
      println("Displaying coalesceHotzoneDf dataframe:")
      coalesceHotzoneDf.show(false)
    }
   
    // return joinDf // YOU NEED TO CHANGE THIS PART

    return coalesceHotzoneDf
  }

}
