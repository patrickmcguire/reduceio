package storm.scala.dsl

import backtype.storm.tuple.{Fields, Tuple, Values}
import backtype.storm.task.TopologyContext
import backtype.storm.task.OutputCollector
import backtype.storm.tuple.MessageId
import collection.JavaConversions._


// A base class for the other DSL classes
abstract class BaseEmitDsl(val collector: OutputCollector) {
  var emitFunc: List[AnyRef] => java.util.List[java.lang.Integer] = collector.emit(_)

  // The emit function takes in a variable list of (arg1, arg2, ...) which looks
  // like a tuple!
  // The args are translated to a list for efficiency.
  def emit(values: AnyRef*) = emitFunc(values.toList)
}


// unanchored emit:
//    new UnanchoredEmit(collector) emit (val1, val2, ...)
// unanchored emit to a specific stream:
//    new UnanchoredEmit(collector) toStream <streamId> emit (va1, val2, ..)
class UnanchoredEmit(collector: OutputCollector) extends BaseEmitDsl(collector) {
  def toStream(streamId: Int) = {
    emitFunc = collector.emit(streamId, _)
    this
  }
}


// A class/DSL for emitting anchored on a single storm tuple, and acking a tuple.
// emit anchored on one StormTuple:
//    stormTuple emit (val1, val2, .. )
// emit anchored on one StormTuple for a stream:
//    stormTuple toStream <streamID> emit (val1, val2, ...)
class StormTuple(collector: OutputCollector, val tuple:Tuple)
  extends BaseEmitDsl(collector) {
  // Default emit function to one that takes in the tuple as the anchor
  emitFunc = collector.emit(tuple, _)

  // stream method causes the emit to emit to a specific stream
  def toStream(streamId: Int) = {
    emitFunc = collector.emit(streamId, tuple, _)
    this
  }

  // Ack this tuple
  def ack = collector.ack(tuple)
}


// A class/DSL for emitting anchored on multiple tuples
//
// multi-anchored emit:
//    List(tuple1,tuple2) emit (val1, val2, ...)
class StormTupleList(collector: OutputCollector, val tuples: List[Tuple])
  extends BaseEmitDsl(collector) {

  emitFunc = collector.emit(tuples, _)

  // There is no interface for emitting to a specific stream anchored on multiple tuples.

  // convenience func for acking a list of tuples
  def ack = tuples foreach { collector.ack }
}