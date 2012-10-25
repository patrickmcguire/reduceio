package storm.scala.matrix

import storm.scala.matrix.Matrix
import scala.io.Source
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D
import scala.util.matching.Regex.Match

class MatrixMarket(filename: String) {
  val lines = Source.fromFile(filename).getLines

  // %%MatrixMarket matrix coordinate real general
  val header = lines.next

  // % 
  val skip = lines.next

  // 165226 3238249 4739662
  // rows columns nonzeros
  val paramParser = """(\d+) (\d+) (\d+)""".r
  val paramParser(rows, columns, nonzeros) = lines.next
  private val _backing = new SparseDoubleMatrix2D(
      rows.toInt, columns.toInt, nonzeros.toInt, 0.1, 0.9)
  private val _matrix = new Matrix(_backing)


  for (cell <- lines) {
    val args = cell.split(" ")
    // file format is 1 indexed
    val rowId = args(0).toInt - 1
    val colId = args(1).toInt - 1
    val value = args(2).toDouble
    _matrix.set(rowId, colId, value)
  }
    
  def matrix = _matrix

}
