package com.kpalka.dataprocessingexercise

import java.time.{ Duration, LocalDateTime }

import com.kpalka.dataprocessingexercise.WindowedElements._
import fs2.{ Pure, Stream }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JoinOpsTest extends AnyFunSuite with Matchers {
  val windowSize  = Duration.ofMinutes(1)
  val windowSlide = Duration.ofMinutes(1)
  test("to windowed elements") {
    val windowSize  = Duration.ofMinutes(1)
    val windowSlide = Duration.ofMinutes(1)
    toWindowedElements(
      Stream(
        LocalDateTime.of(2020, 1, 1, 12, 0, 0),
        LocalDateTime.of(2020, 1, 1, 12, 0, 30),
        LocalDateTime.of(2020, 1, 1, 12, 1, 0),
        LocalDateTime.of(2020, 1, 1, 12, 5, 0)
      )
    )(identity, windowSize, windowSlide).toList.map(_.elements) shouldBe Seq(
      Seq(
        LocalDateTime.of(2020, 1, 1, 12, 0, 0),
        LocalDateTime.of(2020, 1, 1, 12, 0, 30)
      ),
      Seq(
        LocalDateTime.of(2020, 1, 1, 12, 0, 30),
        LocalDateTime.of(2020, 1, 1, 12, 1, 0)
      ),
      Seq(
        LocalDateTime.of(2020, 1, 1, 12, 5, 0)
      )
    )
  }
  test("align windows") {
    val s1 =
      toWindowedElements(
        Stream(
          LocalDateTime.of(2020, 1, 1, 12, 0, 0),
          LocalDateTime.of(2020, 1, 1, 12, 3, 0),
          LocalDateTime.of(2020, 1, 1, 12, 5, 0),
          LocalDateTime.of(2020, 1, 1, 12, 7, 30)
        )
      )(identity, windowSize, windowSlide)
    val s2 =
      toWindowedElements(
        Stream(
          LocalDateTime.of(2020, 1, 1, 12, 0, 30),
          LocalDateTime.of(2020, 1, 1, 12, 3, 0),
          LocalDateTime.of(2020, 1, 1, 12, 6, 0),
          LocalDateTime.of(2020, 1, 1, 12, 7, 0)
        )
      )(identity, windowSize, windowSlide)
    alignWindows(s1, s2).toList.map(_.elements) shouldBe Seq(
      Seq((Seq(LocalDateTime.of(2020, 1, 1, 12, 0, 0)), Seq(LocalDateTime.of(2020, 1, 1, 12, 0, 30)))),
      Seq((Seq(LocalDateTime.of(2020, 1, 1, 12, 3, 0)), Seq(LocalDateTime.of(2020, 1, 1, 12, 3, 0)))),
      Seq((Seq(LocalDateTime.of(2020, 1, 1, 12, 7, 30)), Seq(LocalDateTime.of(2020, 1, 1, 12, 7, 0))))
    )
  }

  test("join using sliding windows") {
    val l1: Stream[Pure, (LocalDateTime, String)] = Stream(
      (LocalDateTime.of(2020, 8, 23, 11, 57, 0), "C"),
      (LocalDateTime.of(2020, 8, 23, 11, 58, 0), "A"),
      (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A"),
      (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "B"),
      (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "C")
    )

    val l2 = Stream(
      (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A"),
      (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "B"),
      (LocalDateTime.of(2020, 8, 23, 12, 1, 0), "C")
    )

    import Join._

    l1.joinUsingSlidingWindow(l2)(
        _._1,
        _._1,
        Duration.ofMinutes(2),
        Duration.ofMinutes(1),
        (a, b) => a._2 == b._2
      )
      .toList shouldBe
      Seq(
        ((LocalDateTime.of(2020, 8, 23, 11, 58, 0), "A"), (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A")),
        ((LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A"), (LocalDateTime.of(2020, 8, 23, 11, 59, 0), "A")),
        ((LocalDateTime.of(2020, 8, 23, 11, 59, 0), "B"), (LocalDateTime.of(2020, 8, 23, 12, 0, 0), "B")),
        ((LocalDateTime.of(2020, 8, 23, 12, 0, 0), "C"), (LocalDateTime.of(2020, 8, 23, 12, 1, 0), "C"))
      )

  }
}
