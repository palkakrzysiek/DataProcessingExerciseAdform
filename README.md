In CSV header of `Views.csv` I've replaced
```
logtime,logtime,campaignid
```
with 
```
id,logtime,campaignid
```

Filename ViewableViews.csv was replaced to ViewableViewEvents.csv to match the task description

Tested with Java 12 and 1.3.13

to check configurable filenames run
```
$ sbt "runMain com.kpalka.dataprocessingexercise.App --help"
[info] running com.kpalka.dataprocessingexercise.App --help
  -c, --clicks-file  <arg>
  -s, --statistics-output  <arg>
      --viewable-view-events-file  <arg>
      --viewable-views-output  <arg>
  -v, --views-file  <arg>
      --views-with-clicks-output  <arg>
  -h, --help                               Show help message
```

Run the application with. 
```
$ sbt "runMain com.kpalka.dataprocessingexercise.App"
```
When non-default filenames are needed, adujst them with additional arguments

```
$ sbt "runMain com.kpalka.dataprocessingexercise.App --clicks-file DifferentClicks.csv"
```
