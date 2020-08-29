package com.kpalka.dataprocessingexercise

import java.time.{ Duration, LocalDateTime }

import com.kpalka.dataprocessingexercise.WindowedElements._
import fs2.Stream
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
}
