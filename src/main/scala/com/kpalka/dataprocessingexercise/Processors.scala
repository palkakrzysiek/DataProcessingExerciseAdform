package com.kpalka.dataprocessingexercise

import java.nio.file.Paths
import java.time.Duration

import cats.effect.{ Blocker, ContextShift, IO, IOApp, Sync }
import com.kpalka.dataprocessingexercise.lazylist.WindowedElements._
import fs2.{ io, text, Pipe, Stream }

object Processors {
  private val size  = Duration.ofHours(12)
  private val slide = Duration.ofHours(12)

  def viewsWithClicks(views: LazyList[View], clicks: LazyList[Click]): LazyList[ViewWithClick] =
    views
      .joinUsingSlidingWindows(clicks)(_.logTime, _.logTime, size, slide, _.id == _.interactionId)
      .map {
        case (view, click) => ViewWithClick(view.id, view.logTime, click.id)
      }

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

  def viewsWithClicks2(implicit cs: ContextShift[IO]) = Stream.resource(Blocker[IO]).flatMap { blocker =>
    deserializeCsv("Views.csv", CsvSeDes.deserializeView, blocker)
      .zip(deserializeCsv("Clicks.csv", CsvSeDes.deserializeClick, blocker))
      // TODO continue with reimplementing joinging in fs2
      .evalMap(x => IO(println(x)))
  }

  def processViewsWithClicks = {
    val views  = IoOps.readCsv("Views.csv").map(CsvSeDes.deserializeView)
    val clicks = IoOps.readCsv("Clicks.csv").map(CsvSeDes.deserializeClick)
//    val joined = viewsWithClicks(views, clicks
//    IoOps.writeLines("ViewsWithClicks.csv", joined.map(CsvSeDes.serializeViewWithClick))
  }

}
