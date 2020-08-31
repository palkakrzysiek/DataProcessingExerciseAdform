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

  def processViewsWithClicks(viewsFilename: String, clicksFilename: String, viewsWithClicksFilename: String)(
    implicit cs: ContextShift[IO]
  ) = Stream.resource(Blocker[IO]).flatMap { blocker =>
    val views  = deserializeCsv(viewsFilename, CsvSeDes.deserializeView, blocker)
    val clicks = deserializeCsv(clicksFilename, CsvSeDes.deserializeClick, blocker)
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
      .through(io.file.writeAll(Paths.get(viewsWithClicksFilename), blocker))
  }

  def processViewableViews(viewsFilename: String, viewableViewEventsFilename: String, viewableViewsFilename: String)(
    implicit cs: ContextShift[IO]
  ) = Stream.resource(Blocker[IO]).flatMap { blocker =>
    val views              = deserializeCsv(viewsFilename, CsvSeDes.deserializeView, blocker)
    val viewableViewEvents = deserializeCsv(viewableViewEventsFilename, CsvSeDes.deserializeViewableViewEvent, blocker)
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
      .through(io.file.writeAll(Paths.get(viewableViewsFilename), blocker))
  }

  private def sumBy[F[_], A, B](stream: Stream[F, A])(by: A => B): Stream[F, Map[B, Int]] =
    stream.fold(Map.empty[B, Int]) {
      case (acc, elem) =>
        val newCounter = acc.getOrElse(by(elem), 0) + 1
        acc + (by(elem) -> newCounter)
    }

  def processStatistics(viewsFilename: String, clicksFilename: String, viewableViewsFilename: String)(
    implicit cs: ContextShift[IO]
  ) =
    Stream.resource(Blocker[IO]).flatMap { blocker =>
      val views                   = deserializeCsv(viewsFilename, CsvSeDes.deserializeView, blocker)
      val clicks                  = deserializeCsv(clicksFilename, CsvSeDes.deserializeClick, blocker)
      val viewableViews           = deserializeCsv(viewableViewsFilename, CsvSeDes.deserializeViewableView, blocker)
      val viewsByCampaign         = sumBy(views)(_.campaignId)
      val clicksByCampaign        = sumBy(clicks)(_.campaignId)
      val viewableViewsByCampaign = sumBy(viewableViews)(_.campaignId)
    }

}
