package storm.scala.matrix

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
  val param_parser = """(\d+) (\d+) (\d+)""".r
  val param_parser(rows, columns, nonzeros) = lines.next
  private val _matrix = new SparseDoubleMatrix2D(
      rows.toInt + 1, columns.toInt + 1, nonzeros.toInt, 0.1, 0.9)

  for (cell <- lines) {
    val args = cell.split(" ")
    val row_id = args(0).toInt
    val col_id = args(1).toInt
    val value = args(2).toDouble
    _matrix.set(row_id, col_id, value)
  }
    
  def matrix = _matrix

}
