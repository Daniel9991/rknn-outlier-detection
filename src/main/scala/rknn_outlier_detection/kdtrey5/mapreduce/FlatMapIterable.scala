package rknn_outlier_detection.kdtrey5.mapreduce

import scala.collection._

class FlatmapIterable[T, U](iter: => Iterable[T], f: T => Iterator[U]) extends Iterable[U] {
  override def iterator: Iterator[U] = new Iterator[U] {
    var _initialized = false
    var _next: U = _
    var _hasNext: Boolean = false
    var _i: Iterator[T] = iter.iterator
    var _nested: Iterator[U] = Iterator()
    private def initialize(): Unit = {
      val _i = iter
      _initialized = true
      calcNext()
    }
    private def calcNext(): Unit = {
      if (_nested.hasNext) {
        _hasNext = true
        _next = _nested.next()
        return
      }
      if (_i.hasNext) {
        _nested = f(_i.next())
        _hasNext = _nested.hasNext
        if (_hasNext) _next = _nested.next()
        return
      }
      _hasNext = false
    }
    override def hasNext = {
      if (!_initialized) calcNext()
      _hasNext
    }
    override def next(): U = {
      if (!_initialized) calcNext()
      if (!_hasNext) throw new Exception("No more elements")
      val next = _next
      calcNext()
      next
    }
  }
}
