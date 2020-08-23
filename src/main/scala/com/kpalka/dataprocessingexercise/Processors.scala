package com.kpalka.dataprocessingexercise

import java.time.Duration

import com.kpalka.dataprocessingexercise.WindowedElements._

object Processors {
  private val size  = Duration.ofHours(12)
  private val slide = Duration.ofHours(12)

  def viewsWithClicks(views: LazyList[View], clicks: LazyList[Click]): LazyList[ViewWithClick] =
    views
      .joinUsingSlidingWindows(clicks)(_.logTime, _.logTime, size, slide, _.id == _.interactionId)
      .map {
        case (view, click) => ViewWithClick(view.id, view.logTime, click.id)
      }

  def processViewsWithClicks = {
    val views  = IoOps.readCsv("Views.csv").map(CsvSeDes.deserializeView)
    val clicks = IoOps.readCsv("Clicks.csv").map(CsvSeDes.deserializeClick)
    val joined = viewsWithClicks(views, clicks)
    IoOps.writeLines("ViewsWithClicks.csv", joined.map(CsvSeDes.serializeViewWithClick))
  }

}
