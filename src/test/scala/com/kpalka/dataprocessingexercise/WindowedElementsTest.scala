package com.kpalka.dataprocessingexercise

import java.time.{ Duration, LocalDateTime }

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WindowedElementsTest extends AnyFunSuite with Matchers {
  import WindowedElements._
  def eq[A](x: A, y: A): Boolean = x == y
  test("inner join") {
    innerJoin(LazyList.empty[Int], LazyList(1))(eq) shouldEqual LazyList.empty[Int]
    innerJoin(LazyList(1), LazyList.empty[Int])(eq) shouldEqual LazyList.empty[Int]
    innerJoin(LazyList(1), LazyList(2))(eq) shouldEqual LazyList.empty[Int]
    innerJoin(LazyList(1, 2, 3), LazyList(2, 3, 4))(eq) shouldEqual LazyList((2, 2), (3, 3))
    innerJoin(LazyList(1, 2, 3, 3), LazyList(2, 3, 4))(eq) shouldEqual LazyList((2, 2), (3, 3), (3, 3))
  }

  val windowSize  = Duration.ofMinutes(1)
  val windowSlide = Duration.ofMinutes(1)
  def mkWindowedElement(LazyListNum: Long) =
    WindowedElement(Window(windowSize, windowSlide, LazyListNum), LazyList(LazyListNum))
  test("align windows") {
//    alignWindows(LazyList.empty, LazyList.empty) shouldEqual LazyList.empty
//    alignWindows(LazyList.empty, LazyList(mkWindowedElement(1))) shouldEqual LazyList.empty
//    alignWindows(LazyList(mkWindowedElement(1)), LazyList.empty) shouldEqual LazyList.empty
    alignWindows(LazyList(mkWindowedElement(1), mkWindowedElement(2)), LazyList(mkWindowedElement(2))) shouldEqual LazyList(
      (LazyList(2), LazyList(2))
    )
    alignWindows(LazyList(mkWindowedElement(2), mkWindowedElement(3)), LazyList(mkWindowedElement(2))) shouldEqual LazyList(
      (LazyList(2), LazyList(2))
    )
    alignWindows(LazyList(mkWindowedElement(2)), LazyList(mkWindowedElement(1), mkWindowedElement(2))) shouldEqual LazyList(
      (LazyList(2), LazyList(2))
    )
    alignWindows(LazyList(mkWindowedElement(2)), LazyList(mkWindowedElement(2), mkWindowedElement(3))) shouldEqual LazyList(
      (LazyList(2), LazyList(2))
    )
  }

  import Window._
  private case class Range(start: LocalDateTime, end: LocalDateTime)
  test("windows containing timestamp") {
    windowsContainingTimestamp(
      slide = Duration.ofMinutes(1),
      size = Duration.ofMinutes(2),
      currentTimestamp = LocalDateTime.of(2020, 8, 23, 12, 0, 0)
    ).map(window => Range(window.rangeStart, window.rangeEnd)) shouldBe Seq(
      Range(LocalDateTime.of(2020, 8, 23, 11, 58, 0), LocalDateTime.of(2020, 8, 23, 12, 0, 0)),
      Range(LocalDateTime.of(2020, 8, 23, 11, 59, 0), LocalDateTime.of(2020, 8, 23, 12, 1, 0)),
      Range(LocalDateTime.of(2020, 8, 23, 12, 0, 0), LocalDateTime.of(2020, 8, 23, 12, 2, 0))
    )
    windowsContainingTimestamp(
      slide = Duration.ofMinutes(1),
      size = Duration.ofMinutes(1),
      currentTimestamp = LocalDateTime.of(2020, 8, 23, 12, 0, 0)
    ).map(window => Range(window.rangeStart, window.rangeEnd)) shouldBe Seq(
      Range(LocalDateTime.of(2020, 8, 23, 11, 59, 30), LocalDateTime.of(2020, 8, 23, 12, 0, 30))
    )
  }

  import WindowedElements._
  test("join using sliding windows") {
    val l1 = (LocalDateTime.of(2020, 8, 23, 11, 57, 0), "C") #::
      (LocalDateTime.of(2020, 8, 23, 11, 58, 0), "A") #::
      (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A") #::
      (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "B") #::
      (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "C") #::
      LazyList.empty

    val l2 = (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A") #::
      (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "B") #::
      (LocalDateTime.of(2020, 8, 23, 12, 1, 0), "C") #::
      LazyList.empty

    l1.joinUsingSlidingWindows(l2)(_._1, _._1, Duration.ofMinutes(2), Duration.ofMinutes(1), (a, b) => a._2 == b._2) shouldBe
      ((LocalDateTime.of(2020, 8, 23, 11, 58, 0), "A"), (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A")) #::
        ((LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A"), (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A")) #::
        ((LocalDateTime.of(2020, 8, 23, 11, 59, 0), "B"), (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "B")) #::
        ((LocalDateTime.of(2020, 8, 23, 12, 0, 0), "C"), (LocalDateTime.of(2020, 8, 23, 12, 1, 0), "C")) #::
        LazyList.empty

  }

}
