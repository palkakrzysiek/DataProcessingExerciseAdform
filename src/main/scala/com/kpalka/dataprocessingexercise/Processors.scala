package com.kpalka.dataprocessingexercise

import java.nio.file.Paths
import java.time.Duration

import cats.effect.{ Blocker, ContextShift, IO }
import fs2.{ io, text, Pipe, Stream }

object Processors {
  private val size  = Duration.ofHours(5)
  private val slide = Duration.ofHours(5)

  def parseCsvWithHeaders[F[_]]: Pipe[F, String, Map[String, String]] = in => {
    val headers  = in.head.map(_.split(","))
    val contents = in.tail.filter(_.nonEmpty).map(_.split(","))
    headers.repeat.zip(contents).map {
      case (keys, values) => keys.zip(values).toMap
    }
  }

  def deserializeCsv[A](filename: String, deserializer: Map[String, String] => A, blocker: Blocker)(
    implicit cs: ContextShift[IO]
  ): Stream[IO, A] =
    io.file
      .readAll[IO](Paths.get(filename), blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .through(parseCsvWithHeaders)
      .map(deserializer)

  def processViewsWithClicks(implicit cs: ContextShift[IO]) = Stream.resource(Blocker[IO]).flatMap { blocker =>
    val views  = deserializeCsv("Views.csv", CsvSeDes.deserializeView, blocker)
    val clicks = deserializeCsv("Clicks.csv", CsvSeDes.deserializeClick, blocker)
    import Join._
    views
      .joinUsingSlidingWindow(clicks)(_.logTime, _.logTime, size, slide, _.id == _.interactionId)
      .evalTap(x => IO(println(x)))
      .map {
        case (view, click) => ViewWithClick(view.id, view.logTime, click.id)
      }
      .map(CsvSeDes.serializeViewWithClick)
      .evalTap(x => IO(println(x)))
      .intersperse("\n")
      .through(text.utf8Encode)
      .through(io.file.writeAll(Paths.get("ViewsWithClicks.csv"), blocker))
  }

  def processViewableViews(implicit cs: ContextShift[IO]) = Stream.resource(Blocker[IO]).flatMap { blocker =>
    val views              = deserializeCsv("Views.csv", CsvSeDes.deserializeView, blocker)
    val viewableViewEvents = deserializeCsv("ViewableViewEvents.csv", CsvSeDes.deserializeViewableViewEvent, blocker)
    import Join._
    views
      .joinUsingSlidingWindow(viewableViewEvents)(_.logTime, _.logTime, size, slide, _.id == _.interactionId)
      .evalTap(x => IO(println(x)))
      .map {
        case (view, viewableViewEvent) =>
          ViewableView(
            viewableViewEvent.id,
            viewableViewEvent.logTime,
            viewableViewEvent.interactionId,
            view.campaignId
          )
      }
      .map(CsvSeDes.serializeViewableView)
      .evalTap(x => IO(println(x)))
      .intersperse("\n")
      .through(text.utf8Encode)
      .through(io.file.writeAll(Paths.get("ViewableViews.csv"), blocker))
  }

}
