package com.kpalka.dataprocessingexercise.stream

import java.time.{ Duration, LocalDateTime }

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import fs2.Stream
import JoinOps._
import WindowedElements._

class JoinOpsTest extends AnyFunSuite with Matchers {
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
    )(identity, windowSize, windowSlide).toList shouldBe List.empty
  }
}
