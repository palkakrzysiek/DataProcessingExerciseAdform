package com.kpalka.dataprocessingexercise

import java.time.{ Duration, LocalDateTime }

import fs2.{ Pull, Stream }

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

  def alignWindows[F[_], A, B](
    as: Stream[F, WindowedElement[A]],
    bs: Stream[F, WindowedElement[B]]
  ): Stream[F, WindowedElement[(Seq[A], Seq[B])]] = {
    // an example of StepLeg loops https://github.com/typelevel/fs2/issues/1678#issuecomment-549569203
    def go(
      as: Stream.StepLeg[F, WindowedElement[A]],
      bs: Stream.StepLeg[F, WindowedElement[B]]
    ): Pull[F, WindowedElement[(Seq[A], Seq[B])], Unit] = {
      // Stream.emit in `go` invocation guarantees the chunks have one element
      val ah = as.head(0)
      val bh = bs.head(0)
      if (ah.window.seqNumber < bh.window.seqNumber) as.stepLeg.flatMap {
        case Some(at) => go(at, bs)
        case None     => Pull.done
      }
      else if (ah.window.seqNumber > bh.window.seqNumber) bs.stepLeg.flatMap {
        case Some(bt) => go(as, bt)
        case None     => Pull.done
      }
      else {
        Pull.output1(WindowedElement(ah.window, Seq((ah.elements, bh.elements)))) >> as.stepLeg.flatMap {
          case Some(at) =>
            bs.stepLeg.flatMap {
              case Some(bt) => go(at, bt)
              case None     => Pull.done
            }
          case None => Pull.done
        }
      }

    }

    /**
     * convenient uncons1 in alignWindows caused <code>
     * java.lang.RuntimeException: Scope lookup failure!
     *
     * This is typically caused by uncons-ing from two or more streams in the same Pull.
     * To do this safely, use `s.pull.stepLeg` instead of `s.pull.uncons` or a variant
     * thereof. See the implementation of `Stream#zipWith_` for an example.
     * </code>
     *
     * This makes the code a bit spaghettish
     *
     */
    as.flatMap(Stream.emit)
      .pull
      .stepLeg
      .flatMap {
        case Some(leg1) =>
          bs.flatMap(Stream.emit).pull.stepLeg.flatMap {
            case Some(leg2) => go(leg1, leg2)
            case None       => Pull.done
          }
        case None => Pull.done
      }
      .stream
  }

  def innerJoin[A, B](as: Seq[A], bs: Seq[B])(predicate: (A, B) => Boolean): Seq[(A, B)] =
    as.flatMap(a => bs.filter(b => predicate(a, b)).map(b => (a, b)))

  def combineWindowedElements[A](
    xs: Seq[WindowedElement[A]],
    ys: Seq[WindowedElement[A]]
  ): Seq[WindowedElement[A]] =
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

object Join {
  import WindowedElements._

  def filterOutDuplicates[F[_], A, B](s: Stream[F, WindowedElement[(A, B)]]): Stream[F, WindowedElement[(A, B)]] =
    s.zipWithPrevious.map {
      case (None, first)      => first
      case (Some(prev), curr) => WindowedElement(curr.window, curr.elements.filterNot(prev.elements.contains))
    }.filter(_.elements.nonEmpty)

  implicit class JoinOps[F[_], A](as: Stream[F, A]) {
    def joinUsingSlidingWindow[B](bs: Stream[F, B])(
      asToTimestamp: A => LocalDateTime,
      bsToTimestamp: B => LocalDateTime,
      size: Duration,
      slide: Duration,
      predicate: (A, B) => Boolean
    ): Stream[F, (A, B)] = {
      val asWindowed = toWindowedElements(as)(asToTimestamp, size, slide)
      val bsWindowed = toWindowedElements(bs)(bsToTimestamp, size, slide)
      val aligned    = alignWindows(asWindowed, bsWindowed)
      val joined = aligned.map {
        case WindowedElement(window, elem) =>
          elem.head match {
            case (as, bs) => WindowedElement(window, innerJoin(as, bs)(predicate))
          }
      }
      filterOutDuplicates(joined)
        .flatMap(windowedElement => Stream.emits(windowedElement.elements))
    }
  }
}
