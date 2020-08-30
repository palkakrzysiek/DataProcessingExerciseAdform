package com.kpalka.dataprocessingexercise

import cats.effect.{ ExitCode, IO, IOApp }
import cats.implicits.catsSyntaxFlatMapOps

object App extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val conf               = new Conf(args)
    val views              = conf.viewsFile.getOrElse("Views.csv")
    val clicks             = conf.clicksFile.getOrElse("Clicks.csv")
    val viewableViewEvents = conf.viewableViewEventsFile.getOrElse("ViewableViewEvents.csv")
    val viewsWithClicks    = conf.viewsWithClicksOutput.getOrElse("ViewsWithClicks.csv")
    val viewableViews      = conf.viewableViewsOutput.getOrElse("ViewableViews.csv")
    val statistics         = conf.statisticsOutput.getOrElse("statistics.csv")
    (Processors.processViewsWithClicks(views, clicks, viewsWithClicks).compile.drain >>
      Processors.processViewableViews(views, viewableViewEvents, viewableViews).compile.drain).as(ExitCode.Success)
  }
}
