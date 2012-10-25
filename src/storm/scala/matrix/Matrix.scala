package storm.scala.matrix

import cern.colt.matrix.tdouble.DoubleMatrix2D
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D

class Matrix(matrix: DoubleMatrix2D) {

  private val _matrix = matrix

  def this(rows: Int, columns: Int) = this(new SparseDoubleMatrix2D(rows, columns))
    
  def mult(otherMatrix: Matrix):DoubleMatrix2D = {
    if (otherMatrix.rows != _matrix.columns) {
      // throw an error
    }

    val resultMatrix = new SparseDoubleMatrix2D(_matrix.rows, otherMatrix.columns)
    _matrix.zMult(otherMatrix.backing, resultMatrix) 
    return resultMatrix
  }

  def square():DoubleMatrix2D = {
    val resultMatrix = new SparseDoubleMatrix2D(_matrix.rows, matrix.rows)
    return _matrix.zMult(_matrix, resultMatrix, 1.0, 1.0, false, true)
  }

  def backing = _matrix

  def rows = _matrix.rows

  def columns = _matrix.columns

  def set(x:Int, y:Int, value:Double) = _matrix.set(x, y, value)

  def viewPart(row: Int, column: Int,
               height: Int, width: Int) = new Matrix(_matrix.viewPart(row, column, height, width))
}
