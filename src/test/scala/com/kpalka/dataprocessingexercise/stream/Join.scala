package com.kpalka.dataprocessingexercise.stream

import java.time.{ Duration, LocalDateTime }

import com.kpalka.dataprocessingexercise.lazylist.{ Window, WindowedElement }
import fs2.Stream

object Join {}

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

case class Window(size: Duration, slide: Duration, seqNumber: Long) {
  def rangeMidpoint: LocalDateTime = Window.globalStartTimestamp.plus(slide.multipliedBy(seqNumber))
  def rangeStart: LocalDateTime    = rangeMidpoint.minus(size.dividedBy(2))
  def rangeEnd: LocalDateTime      = rangeMidpoint.plus(size.dividedBy(2))
  def isTimestampWithinWindow(timestamp: LocalDateTime): Boolean =
    (timestamp.isEqual(rangeStart) || timestamp.isAfter(rangeStart)) && (timestamp.isEqual(rangeEnd) || timestamp
      .isBefore(rangeEnd))
}

case class WindowedElement[A](window: Window, elements: Seq[A])
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

  def toWindowedElements[F[_], A](
    as: Stream[F, A]
  )(timestampExtractor: A => LocalDateTime, size: Duration, slide: Duration): Stream[F, WindowedElement[A]] = {
    val windowed = as.scan(WindowingState.empty[A]) { (state, currentWindow) =>
      val timestamp                = timestampExtractor(currentWindow)
      val windows                  = Window.windowsContainingTimestamp(slide, size, timestamp)
      val windowedElements         = windows.map(window => WindowedElement(window, Seq(currentWindow)))
      val combinedWindowedElements = combineWindowedElements(state.windowsBeingProcessed, windowedElements)
      val (toBeEmitted, stillBeingProcessed) =
        combinedWindowedElements.partition(_.window.rangeEnd.isBefore(timestamp))
      WindowingState(stillBeingProcessed, toBeEmitted)
    }
    windowed.zipWithNext.flatMap {
      case (windowingState, Some(_)) => Stream.emits(windowingState.windowsToBeEmitted)
      case (windowingState, None) => // flush the rest of the windows being processed on the last element
        Stream.emits(windowingState.windowsToBeEmitted) ++ Stream.emits(windowingState.windowsBeingProcessed)
    }
  }
}

object JoinOps {

  implicit class JoinOps[F[_], A](as: Stream[F, A]) {
    def joinUsingSlidingWindow[B](bs: Stream[F, B]): Stream[F, (A, B)] =
      ???

  }
}
