package storm.scala.matrix
/*
import cern.colt.matrix.impl.SparseDoubleMatrix2D

class Matrix(rows: Int, columns: Int) {

  var backing: Map[(Int,Int),cern.colt.matrix.impl.SparseDoubleMatrix2D] = _
  if (rows.toLong * columns.toLong < java.lang.integer.MAX_VALUE) {
    backing = Map((0,0) -> new cern.colt.matrix.impl.SparseDoubleMatrix2D(rows, columns))
  } else {
    val hugenessRatio = (rows.toLong * columns.toLong) / java.lang.integer.MAX_VALUE
    val linearScale = scala.math.sqrt(hugenessRation).toInt + 1
    val iterator = (0 until linearScale).iterator.zipAll((0 until linearScale).iterator)
  }
}
*/
