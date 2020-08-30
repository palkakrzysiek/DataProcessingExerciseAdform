package com.kpalka.dataprocessingexercise

import cats.effect.{ ExitCode, IO, IOApp }

object App extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    Processors.processViewableViews.compile.drain.as(ExitCode.Success)
}
