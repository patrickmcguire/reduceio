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

object SplitSize {
  val Default = 1000
}

class DualMatrixSpout(m1FilePath: String, m2FilePath: String,
                      m1Transpose: Boolean, m2Transpose: Boolean) 
  extends StormSpout(outputFields = List("rowFirst", "rowLast", 
                                         "columnFirst", "columnLast",
                                         "m1Rows", "m2Columns")) {

  var zipped: ListBuffer[(Int,Int)] = _
  var zippedIterator: Iterator[(Int,Int)] = _
  var m1: Matrix = _
  var m2: Matrix = _


  setup {
    val market1 = new MatrixMarket(m1FilePath)
    if (m1Transpose) {
      m1 = market1.matrix
    } else {
      m1 = market1.matrix.transpose
    }

    val market2 = new MatrixMarket(m2FilePath)
    if (m2Transpose) {
      m2 = market2.matrix
    } else {
      m2 = market2.matrix.transpose
    }

    zipped = new ListBuffer[(Int,Int)]
    for (i <- 0 until (m1.rows / SplitSize.Default)) {
      for (j <- 0 until (m2.columns / SplitSize.Default)) {
        zipped += Tuple2(i,j)
      }
    }
    zippedIterator = zipped.iterator
  }

  def nextTuple {
    if (zippedIterator.hasNext) {
      val blockCoordinates = zippedIterator.next
      val rowCoordinate = blockCoordinates._1
      val columnCoordinate = blockCoordinates._2
      
      val rowFirst = rowCoordinate * SplitSize.Default
      val columnFirst = columnCoordinate * SplitSize.Default

      var rowLast = rowFirst + SplitSize.Default - 1
      if (rowLast > m1.rows) { // the last
        rowLast = m1.rows - 1
      } 

      var columnLast = columnFirst + SplitSize.Default - 1
      if (columnLast > m2.columns) { // the last
        columnLast = m2.columns - 1
      }

      println("Rows are a " + 
             (rowLast - rowFirst).toString + " by " +
             (m1.columns).toString + " matrix")

      println("Columns are a " +
             (m2.rows).toString + " by " +
             (columnLast - columnFirst).toString + " matrix")
      
      using msgId(Random.nextInt) emit(
        rowFirst: java.lang.Integer,
        rowLast: java.lang.Integer,
        columnFirst: java.lang.Integer,
        columnLast: java.lang.Integer,
        m1.viewPart(rowFirst, 0, rowLast - rowFirst, m1.columns),
        m2.viewPart(0, columnFirst, m2.rows, columnLast - columnFirst)
      )
    }
  }
}

class MatrixBlockMult extends StormBolt(List("rowFirst", "rowLast", 
                                             "columnFirst",  "columnLast", 
                                             "result")) {

  def execute(t: Tuple) = t matchSeq {
    case Seq(rowFirst: Int,
             rowLast: Int,
             columnFirst: Int,
             columnLast: Int,
             m1Rows: Matrix,
             m2Columns: Matrix) => 
        using anchor t toStream "rowFirst" emit (rowFirst)
        using anchor t toStream "rowLast" emit (rowLast)
        using anchor t toStream "columnFirst" emit (columnFirst)
        using anchor t toStream "columnLast" emit (columnLast)
        val target = m1Rows.mult(m2Columns)
        for (r <- 0 until target.rows) {
          for (c <- 0 until target.columns) {
            println(r.toString + "," + c.toString + "," +
              target.get(r,c))
          }
        }
        using anchor t toStream "result" emit target
        t ack
    case _ => {throw new Exception("Invalid tuple type")}
  }
}

class MatrixBlockMerge extends StormBolt(List("rowFirst", "rowLast",
                                              "columnFirst", "columnLast", "result")) {

  def execute(t: Tuple) = t matchSeq {
    case _ => {}
  }
}


object MatrixTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder
    val filepath = "/home/patrick/Code/hivemind/count.mtx"
    builder.setSpout("matrix-spewer", new DualMatrixSpout(
      filepath, filepath, false, true))
    builder.setBolt("matrix-block-mult", new MatrixBlockMult, 8)
      .shuffleGrouping("matrix-spewer")

    val conf = new Config
    conf.setDebug(true)
    conf.setMaxTaskParallelism(8)

    val cluster = new LocalCluster
    cluster.submitTopology("matrix-mult", conf, builder.createTopology)
    Thread sleep 10000
    cluster.shutdown
  }
}
