package com.quan.util

import com.quan.context.AppContext
import com.quan.model.Cell
import breeze.linalg._

object SOMHelper {
  /**
    *
    * @param numRows
    * @param numCols
    * @return
    */
  def createCells(numRows: Int, numCols: Int): Array[Array[Cell]] = {
    val temp = for (row <- 0 to numRows)
      yield (
        for (col <- 0 to numCols)
          yield new Cell(row, col)
        ).toArray
    temp.toArray
  }


}
