#Job template for Spark/Scala jobs
This project is a baseline for basically aby Spark/Scala processing job implementing certain ETL logic. It considers the following elements:
1) Configuration stored in src/main|test/resources capable of handling JSON conf. file, reading parameters from environment variables and getting them as JVM arguments (-D switch when executing java program)
2) Logger (as an instance of slf4j) getting configuration from ***src/main|test/resources/log4j.properties***. By default spark log messages are displayed on the level ERROR. For internal logic level INFO and higher is shown.
3) Spark logic is structured in a following manner: Readers -> stage logic (data preparation, validation) -> processing code (join, aggregates...) -> writes. For each stage there is a dedicated class instance to separate the flow logically and make the code as much readable as possible.

##How to create your own job
This project is designed as a template so please don't modify it unless absolutely necessary. Creating your own one is fairly easy and there are two ways:
1) just ***fork*** it on the left right side of the GitLab Project Window. Select the namespace where to put it and then rename it.
2) ***clone it*** (from IDEA), create a new project in GitLab then change remote to this new repository in the cloned instance.

##Dependencies
Dependencies are handled in the build.gradle file. There are all the important once - spark, scala, pure config and logger. Also it includes the spark_common library containing readers, writers and helpers with configuration fetched from the internal nexus 3 repository. To be able to get them it is necessary to have access to the CDCP workbench and also gradle.properties in your home directory (folder .gradle) must be set according to the workbench template.
##Configuration
##Logging
##Processing structure
##Tests 