package storm.scala.matrix

import scala.io.Source
import cern.colt.matrix.impl.SparseDoubleMatrix2D
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
  val matrix = new SparseDoubleMatrix2D(
      rows.toInt, columns.toInt, nonzeros.toInt, 0.0, 1.0)

  for (cell <- lines) {
    val args = cell.split(" ")
    val row_id = args(0).toInt
    val col_id = args(1).toInt
    val value = args(2).toDouble
    matrix.set(row_id, col_id, value)
  }
    
  def getMatrix: SparseDoubleMatrix2D = matrix

}
