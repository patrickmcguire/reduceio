package storm.scala.matrix

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import util.Random
import redis.clients.jedis._
import scala.util.matching.Regex.Match
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import cern.colt.matrix.impl.SparseDoubleMatrix2D
import storm.scala.matrix.MatrixMarket

class FileMatrixSpout(filename: String, concurrency: Integer) 
  extends StormSpout(List("row_indices", "column_indices", "rows", "columns")) {

  var matrix: cern.colt.matrix.impl.SparseDoubleMatrix2D = _
  var linearDivision: Integer = _
  var zippedIterator: scala.collection.Iterator[(Int,Int)] = _

  def setup {
    val matrixMarket = new storm.scala.matrix.MatrixMarket(filename)
    matrix = matrixMarket.getMatrix
    linearDivision = scala.math.sqrt(concurrency.toDouble).toInt
    val rowIterator = (0 until linearDivision).iterator
    val columnIterator = (0 until linearDivision).iterator
    zippedIterator = rowIterator.zipAll(columnIterator,-1,-1)
  }

  def nextTuple {
    if (zippedIterator.hasNext) {
      val blockCoordinates = zippedIterator.next
      val rowCoordinate = blockCoordinates._1
      val columnCoordinate = blockCoordinates._2
      
      val rowFirst = (matrix.rows / linearDivision) *
        rowCoordinate

      val columnFirst = (matrix.columns / linearDivision) * 
        columnCoordinate

      var rowLast = 0
      if (rowFirst == linearDivision - 1) { // the last
        rowLast = matrix.rows - 1
      } else {
        rowLast = rowFirst +  
          (matrix.rows / linearDivision) - 1
      }

      var columnLast = 0
      if (columnFirst == linearDivision - 1) { // the last
        columnLast = matrix.columns - 1
      } else {
        columnLast = columnFirst + 
          (matrix.columns / linearDivision) - 1
      }
      emit(List(
        rowFirst until (rowLast + 1),
        columnFirst until (columnLast + 1),
        matrix.viewPart(rowFirst, rowFirst - rowLast, 0, matrix.columns),
        matrix.viewPart(0, matrix.rows, columnFirst, columnFirst - columnLast)
      ))
    }
  }
}

class RedisMatrixSpout extends StormSpout(outputFields = List("rownum", "row")) {
  var rowkey: scala.util.matching.Regex = _
  var it: Iterator[String] = _
  var redis_conn: Jedis = _
  var keys: scala.collection.mutable.Set[String] = _
  var buff: ListBuffer[(Int, String)] = _

  setup {
    rowkey = new scala.util.matching.Regex("""matrix:row:(\d*)""", "row")
    redis_conn = new Jedis("localhost")
    keys = redis_conn.keys("matrix:row:*")
    it = keys.iterator
    buff = new ListBuffer[(Int, String)]
  }
  
  def nextTuple {
    if (it.hasNext) {
      val key = it.next
      val r = key match {
        case rowkey(row) => row.toInt
        case _ => -1
      }
      val tup = Tuple2(r, redis_conn.get("matrix:row:" + r.toString))
      buff += tup
      println(buff)
      using msgId(Random.nextInt) emit (tup)
    } else {
      using msgId(Random.nextInt) emit (List(-1,""))
    }
  }
}

class MatrixPrint extends StormBolt(List("r1", "c1", "v1", "r2", "c2", "v2")) {
  var counts: HashMap[Int, String] = _
  setup {
    counts = new HashMap[Int, String]()
  }

  def execute(t: Tuple){
    t matchSeq {
      case Seq(row_id: Int, row: String) => new Tuple2(row_id, row)
      counts += new Tuple2(row_id, row)
      using anchor t toStream "row_id" emit (row_id)
      using anchor t toStream "row" emit (row)
      t ack
    }
  }
  
  /*
   * Summa algorith for total morons
   * I, J represent all rows, columns owned by processor
   * k is single row or column
   *   - or block of b rows or columbs
   * C(I,J) = C(I,J) + sum_k A(I,k)*B(k,J)
   *
   * for k=(0..n-1)
   *   for all I = 1 to p_r // in parallel
   *      owner of A(I,k) broadcasts it to whole processor row
   *   for all J = 1 to p_c // in parallel
   *      owner of B(k,J) broadcasts it to whole processor column
   *   Receive A(I,k) into Acol
   *   Receive B(k,j) into Brow
   *
   *   C(myproc, myproc) = C(myproc, myproc) + Acol * Brow
   */
}


object MatrixTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder
    builder.setSpout("matrix", new RedisMatrixSpout, 1)
    builder.setBolt("mult", new MatrixPrint, 1)

    val conf = new Config
    conf.setDebug(true)
    conf.setMaxTaskParallelism(1)

    val cluster = new LocalCluster
    cluster.submitTopology("matrix-mult", conf, builder.createTopology)
    Thread sleep 10000
    cluster.shutdown
  }
}
