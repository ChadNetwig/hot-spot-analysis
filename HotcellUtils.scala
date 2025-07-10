/*
CLN: Added helper function calcSpatialWeight to calculate a cell's spatial weight with respect to adjacent cells
*/
package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

  // CLN: implementation of helper functions in the HotcellUtils object

  /**
   * CLN: Calculates the spatial weight for neighboring cells based on their coordinates
   *
   * @param x1    The x-coordinate of the first cell.
   * @param y1    The y-coordinate of the first cell.
   * @param z1    The z-coordinate of the first cell.
   * @param minX  The minimum x-coordinate of the cube.
   * @param maxX  The maximum x-coordinate of the cube.
   * @param minY  The minimum y-coordinate of the cube.
   * @param maxY  The maximum y-coordinate of the cube.
   * @param minZ  The minimum z-coordinate of the cube.
   * @param maxZ  The maximum z-coordinate of the cube.
   * @return      The spatial weight, representing the number of neighboring cells.
   * 
   * NOTE: A weight of 1 is given to the cell for each of its neighbors in the 3x3 space-time cube.
   *       The spatial neighborhood is created for the preceeding, current, and following time periods (i.e., each cell has 26
   *       neighbors, unless it is on a dimension boundary)
   */
  def calcSpatialWeight(x1: Int, y1: Int, z1: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    var count = 0

    // Check if the cell is on the boundary of the x-dimension
    if (x1 == minX || x1 == maxX) { count += 1 }

    // Check if the cell is on the boundary of the y-dimension
    if (y1 == minY || y1 == maxY) { count += 1 }

    // Check if the cell is on the boundary of the z-dimension
    if (z1 == minZ || z1 == maxZ) { count += 1 }

    // If the cell is located on the boundary of exactly one dimension (x, y, or z), it will have 17 neighboring cells. 
    // This includes the cells in the same dimension adjacent to it, as well as the cells diagonally adjacent to it within the 3x3 cube.
    if (count == 1) {
      return 17

    // If the cell is located on the boundary of exactly two dimensions, it will have 11 neighboring cells. This includes the cells in the
    // remaining dimension adjacent to it, as well as the cells diagonally adjacent to it within the 3x3 cube
    } else if (count == 2) {
      return 11

    // If the cell lies on the boundary of all three dimensions (x, y, and z), count will be 3. Returns a weight of 7. 
    } else if (count == 3) {
      return 7

    // If the cell is located in the middle of the 3x3 cube, it will have 26 neighboring cells, considering all the cells within the cube.
    } else {
      return 26

    }
  } // close calcSpatialWeight



} // close HotCellUtils object
