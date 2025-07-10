package cse512

// CLN: 06-26-23 Implemented ST_Contains to check if the point is in the rectangle and return bool
object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // Splitting the rectangle and point coordinates
    val rectangleCoordinates = queryRectangle.split(",")
    val pointCoordinates = pointString.split(",")

    // Extracting individual coordinates
    val rectX1 = rectangleCoordinates(0).toDouble
    val rectY1 = rectangleCoordinates(1).toDouble
    val rectX2 = rectangleCoordinates(2).toDouble
    val rectY2 = rectangleCoordinates(3).toDouble

    val pointX = pointCoordinates(0).toDouble
    val pointY = pointCoordinates(1).toDouble

    // Checking if the point is inside the rectangle
    // val contains = pointX >= rectX1 && pointX <= rectX2 && pointY >= rectY1 && pointY <= rectY2
    val contains = (pointX >= rectX1) && (pointX <= rectX2) && (pointY >= rectY1) && (pointY <= rectY2)


    // Returning the result
    return contains
  }


}

