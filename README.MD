# Job template for Spark/Scala jobs
This project is a baseline for basically any Spark/Scala processing job implementing certain ETL logic. It considers the following elements:
1) Configuration stored in src/main|test/resources capable of handling JSON conf. file, reading parameters from environment variables and getting them as JVM arguments (-D switch when executing java program)
2) Logger (as an instance of slf4j) getting configuration from ***src/main|test/resources/log4j.properties***. By default spark log messages are displayed on the level ERROR. For internal logic level INFO and higher is shown.
3) Spark logic is structured in a following manner: Readers -> stage logic (data preparation, validation) -> processing code (join, aggregates...) -> writes. For each stage there is a dedicated class instance to separate the flow logically and make the code as much readable as possible.

## How to create your own job
This project is designed as a template so please don't modify it unless absolutely necessary. Creating your own one is fairly easy and there are two ways:
1) just ***fork*** it on the left right side of the GitLab Project Window. Select the namespace where to put it and then rename it. Also it's worth to brake the fork link with this source - Settings-> general -> remove fork relationship
2) ***clone it*** (from IDEA), create a new project in GitLab then change remote to this new repository in your cloned instance.

## Dependencies
Dependencies are handled in the build.gradle file. There are all the important ones - spark, scala, pure config and logger. Also it includes the spark_common library containing readers, writers and helpers with configuration fetched from the internal nexus 3 repository. To be able to get them it is necessary to have access to the CDCP workbench and also gradle.properties in your home directory (folder .gradle) must be set according to the workbench template.
## Configuration
Configuration is implemented using scala pure-config you can do the following to specify your job's parameters:
- JSON file in src/main|test/resources/<your_config_file> here ***job_template.conf***. JSON format is used and you can place it elsewhere - more below.
- Environment variable
- JVM parameter - specified using -D java switch
### Configuration handling implementation
Implementation can be found in the package ***config***. There are basically two classes:
1) Settings - containing a case class to wrap all the parameters at one place.
2) Setup - a module for handling config retrieval. There are two main part - in the class parameter you can specify the name of your configuration file. Later you are defining how parameters are mapped to the Settings case class.
Both classes inherit from spark_common which contain some useful features for parameter validation and print.
## Logging
For logging there is only one configuration file again in the resources folder (one in main and one in test). You can specify logging levels per package. Spark and other supporting libraries are set to log ERRORs, logs from com.tmobile.sit packages log LOG level and higher. For logging SLF4J is used, logger instantiation is inherited from the spark_common package.
## Processing structure
Implementation of the processing structure is designed in couple of blocks. The main reason is testability and readability of the implemented code. Generally it's not a good idea to have all the logic (reading data, staging, processing, writing) in one single block and method. The reasons are obvious - you cant test the code part by part and its quite easy to get lost while reading because logic will probably be quite long piece of text. 
It's therefore very wise having the logic structured in couple of blocks. Each block should be implemented in a separated class(es) to structure the code logically. General advice would be the following:
1) Readers - ideally use the ones implemented in the spark_common library. All implement Reader trait so data retrieval is done by calling method read which returns dataframe. You can easily MOCK them in your test by implementing the Reader yourself.
2) Stage processing - used for data validation, filtering, simple mapping etc. Again this should be one or more classes getting dataframe(s) from readers and returning dataframe again.
3) Processing Core - the main block for the code of data transformations. Again should be implemented in a separated class(es). Input should be dataframes from stage, output is again a set of dataframes. In case you have more data sources it's worth considering case classes as a wrappers for a group of dataframes. 
4) Writers - classes for writing data, consuming dataframes from Processing Core. You can use spark_common library to get writers for common formats. They again implement Writer trait and so can be easily MOCKed and used for testing then.
5) Orchestration - how to combine all above? The suggested way is to implement an orchestrator class (here called Pipeline) which takes all the structures above (Readers, stage, code...) and class parameters and then chain them together creating processing flow.
6) Execution - done in the Processor class which creates all the instances for blocks 1-4 and then passes them to the orchestrator part (5) and executes the processing. Also it should validate and generally handle parameters and create sparkSession. SparkSession is created in the package object. 
## Tests 
It's very important to test all the functionality. In the Test folder you should write unit tests for all your functions and classes. One small test is suggested there for this template job as well. It uses the standard Scala test libraries together with Holden Karau's library for dataframe verification called DataframeSuiteBase. The main principle is to either mock readers or create input dataframes manually, then pass them to the processing block one by one (method by method) and verify them by comparing results with dataframes specified in the ReferenceData object. Each method and class used in the processing should have couple of tests for verification.