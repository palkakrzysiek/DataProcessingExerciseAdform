package com.kpalka.dataprocessingexercise.lazylist

import java.time.{ Duration, LocalDateTime }

import com.kpalka.dataprocessingexercise.lazylist.Window.globalStartTimestamp

import scala.collection.immutable.LazyList.toDeferrer
import scala.collection.immutable.Queue

case class WindowedElement[A](window: Window, elements: Seq[A])

case class Window(size: Duration, slide: Duration, seqNumber: Long) {
  def rangeMidpoint: LocalDateTime = globalStartTimestamp.plus(slide.multipliedBy(seqNumber))
  def rangeStart: LocalDateTime    = rangeMidpoint.minus(size.dividedBy(2))
  def rangeEnd: LocalDateTime      = rangeMidpoint.plus(size.dividedBy(2))
  def isTimestampWithinWindow(timestamp: LocalDateTime): Boolean =
    (timestamp.isEqual(rangeStart) || timestamp.isAfter(rangeStart)) && (timestamp.isEqual(rangeEnd) || timestamp
      .isBefore(rangeEnd))
}

object Window {
  val globalStartTimestamp: LocalDateTime = LocalDateTime.of(2010, 1, 1, 0, 0)
  def windowsContainingTimestamp(slide: Duration, size: Duration, currentTimestamp: LocalDateTime): Seq[Window] = {

    val firstWindowTimestampOffset =
      Duration.between(globalStartTimestamp, currentTimestamp.minus(size.dividedBy(2))).plus(slide)
    // firstWindowTimestampOffset = startTimestamp + firstSeqNum * slide + size / 2
    // firstSeqNum = -(startTimestampOffset + size / 2 - firstWindowTimestampOffset)/slide
    // assuming startTimestampOffset = 0
    // firstSeqNum = -(size / 2 - firstWindowTimestampOffset)/slide
    // firstSeqNum = (-size / 2 + firstWindowTimestampOffset)/slide
    val firstSeq = size.dividedBy(-2).plus(firstWindowTimestampOffset).dividedBy(slide)
    LazyList
      .from(0)
      .map(increment => Window(size, slide, firstSeq + increment))
      .takeWhile(_.isTimestampWithinWindow(currentTimestamp))
  }
}

object WindowedElements {

  def alignWindows[A, B](
    as: => LazyList[WindowedElement[A]],
    bs: => LazyList[WindowedElement[B]]
  ): LazyList[WindowedElement[(Seq[A], Seq[B])]] =
    if (as.isEmpty || bs.isEmpty) LazyList.empty
    else
      (as, bs) match {
        case (aHead #:: aTail, bHead #:: bTail) =>
          if (aHead.window.seqNumber < bHead.window.seqNumber) alignWindows(aTail, bs)
          else if (aHead.window.seqNumber > bHead.window.seqNumber) alignWindows(as, bTail)
          else WindowedElement(aHead.window, Seq((aHead.elements, bHead.elements))) #:: alignWindows(aTail, bTail)
      }

  def innerJoin[A, B](as: Seq[A], bs: Seq[B])(predicate: (A, B) => Boolean): Seq[(A, B)] =
    as.flatMap(a => bs.filter(b => predicate(a, b)).map(b => (a, b)))

  def combineWindowedElements[A](xs: Seq[WindowedElement[A]], ys: Seq[WindowedElement[A]]): Seq[WindowedElement[A]] =
    (xs ++ ys)
      .groupBy(_.window)
      .map {
        case (window, elements) =>
          val combinedElements = elements.flatMap(_.elements)
          WindowedElement(window, combinedElements.distinct)
      }
      .toSeq
      .sortBy(_.window.seqNumber)

  private case class WindowingState[A](
    windowsBeingProcessed: Seq[WindowedElement[A]],
    windowsToBeEmitted: Seq[WindowedElement[A]]
  )
  private object WindowingState {
    def empty[A]: WindowingState[A] = WindowingState(
      Seq.empty[WindowedElement[A]],
      Seq.empty[WindowedElement[A]]
    )
  }
  def toWindowedElements[A](
    as: LazyList[A]
  )(timestampExtractor: A => LocalDateTime, size: Duration, slide: Duration): LazyList[WindowedElement[A]] = {
    val windowed = as.scanLeft(WindowingState.empty[A]) { (state, currentWindow) =>
      val timestamp                = timestampExtractor(currentWindow)
      val windows                  = Window.windowsContainingTimestamp(slide, size, timestamp)
      val windowedElements         = windows.map(window => WindowedElement(window, Seq(currentWindow)))
      val combinedWindowedElements = combineWindowedElements(state.windowsBeingProcessed, windowedElements)
      val (toBeEmitted, stillBeingProcessed) =
        combinedWindowedElements.partition(_.window.rangeEnd.isBefore(timestamp))
      WindowingState(stillBeingProcessed, toBeEmitted)
    }
    windowed.flatMap(_.windowsToBeEmitted) #::: LazyList.from(windowed.last.windowsBeingProcessed)
  }

  def filterOutDuplicates[A](
    stream: LazyList[WindowedElement[A]],
    size: Duration,
    slide: Duration
  ): LazyList[WindowedElement[A]] = {
    val slideSec            = slide.toSeconds
    val sizeSec             = size.toSeconds
    val noOverlapPosibility = slideSec > sizeSec
    if (noOverlapPosibility) stream
    else {
      val maxNumberOfSubsequentWindows = Math.ceil((sizeSec + 1).toDouble / slideSec).toLong // +1 to accommodate for start/end range overlap
      def loop(
        buffer: Queue[WindowedElement[A]],
        remainingStream: => LazyList[WindowedElement[A]]
      ): LazyList[WindowedElement[A]] =
        if (buffer.isEmpty && remainingStream.isEmpty) LazyList.empty
        else if (buffer.size > maxNumberOfSubsequentWindows || buffer.nonEmpty && remainingStream.isEmpty) {
          val (oldest, remainingBuffer) = buffer.dequeue
          val remainingBufferWithoutDuplicates = remainingBuffer.map(windowedElement =>
            WindowedElement(
              windowedElement.window,
              windowedElement.elements.filterNot(oldest.elements.contains)
            )
          )
          oldest #:: loop(remainingBufferWithoutDuplicates, remainingStream)
        } else
          remainingStream match {
            case head #:: tail => loop(buffer.enqueue(head), tail)
          }
      loop(Queue.empty, stream)
    }
  }

  implicit class WindowingOps[A](xs: LazyList[A]) {

    def joinUsingSlidingWindows[B](ys: LazyList[B])(
      xsToTimestamp: A => LocalDateTime,
      ysToTimestamp: B => LocalDateTime,
      size: Duration,
      slide: Duration,
      predicate: (A, B) => Boolean
    ): LazyList[(A, B)] = {
      val xsWindowed                                                  = toWindowedElements(xs)(xsToTimestamp, size, slide)
      val ysWindowed                                                  = toWindowedElements(ys)(ysToTimestamp, size, slide)
      val windowsAligned: LazyList[WindowedElement[(Seq[A], Seq[B])]] = alignWindows(xsWindowed, ysWindowed)
      val joined = windowsAligned.map {
        case WindowedElement(window, elem) =>
          elem.head match {
            case (as, bs) => WindowedElement(window, innerJoin(as, bs)(predicate))
          }
      }
      filterOutDuplicates(joined, size, slide)
        .filter(_.elements.nonEmpty)
        .flatMap(_.elements)
    }
  }

}
