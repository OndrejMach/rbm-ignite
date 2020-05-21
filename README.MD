# RCS RBM Spark processing
This project is the implementation of the ETL layer for Rich Business Messaging data processing. It is written in Scala using the Spark framework for distributed processing. 

The following are considered:
1) Configuration variables are stored in ***src/main|test/resources/rbm_config.linux|windows.conf*** as JSON conf. files. Default configurations can be overwritten by JVM arguments (-D switch when executing java program). There are two files for Windows and Linux paths and the system is detected automatically at runtime.

2) Logger (as an instance of slf4j) getting configuration from ***src/main|test/resources/log4j.properties***. By default spark log messages are displayed on the level ERROR. For internal logic level INFO and higher is shown.

3) Spark logic is structured in the following manner: Setup -> Input (readers) -> Stage logic (data preparation, validation) -> Processing Core (join, aggregates, dimensional/fact processing) -> Output (writers).

## Dependencies
Dependencies are handled in the build.gradle file. There are all the important ones - spark, scala, pure config and logger. 

Also it includes the ***spark_common*** library containing readers, writers and helpers with configuration fetched from the internal nexus 3 repository. 

To be able to get them it is necessary to have access to the CDCP workbench and also ***gradle.properties*** in your home directory (folder .gradle) must be set according to the workbench template.

## Logging
For logging there is only one configuration file again in the resources folder (one in main and one in test). You can specify logging levels per package. Spark and other supporting libraries are set to log ERRORs, logs from com.tmobile.sit packages log LOG level and higher. For logging SLF4J is used, logger instantiation is inherited from the spark_common package.

## Configuration handling implementation
Implementation can be found in the package ***config***. There are basically two classes:
1) Settings - containing a case class to wrap all the parameters at one place.
2) Setup - a module for handling config retrieval. There are two main part - in the class parameter you can specify the name of your configuration file. Later you are defining how parameters are mapped to the Settings case class.
Both classes inherit from spark_common which contain some useful features for parameter validation and print.

## Processing structure
Implementation of the processing structure is designed in several logical blocks. The main reason is testability and readability of the implemented code. Generally it's not a good idea to have all the logic (reading data, staging, processing, writing) in one single block and method. 
It's therefore very wise having the logic structured in couple of blocks. Each block should be implemented in a separated class(es) to structure the code logically. General advice would be the following:
1. **Readers** - ideally use the ones implemented in the spark_common library. All implement Reader trait so data retrieval is done by calling method read which returns dataframe. You can easily MOCK them in your test by implementing the Reader yourself.
2. **Stage processing** - used for data validation, filtering, simple mapping etc. Again this should be one or more classes getting dataframe(s) from readers and returning dataframe again.
3. **Processing Core** - the main block for the code of data transformations. Again should be implemented in a separated class(es). Input should be dataframes from stage, output is again a set of dataframes. In case you have more data sources it's worth considering case classes as a wrappers for a group of dataframes.

    3.1 **DimensionProcessing** - Class to handle processing of dimensional data based on input files
    
    3.2 **FactProcessing** - Class to handle processing of fact tables based on daily and aggregated data

    3.3 **PersistenceHandling** - Class to handle aggregating and persistent data which requires updating some output

4. **Writers** - classes for writing data, consuming dataframes from Processing Core. You can use spark_common library to get writers for common formats. They again implement Writer trait and so can be easily MOCKed and used for testing then.

5. **Orchestration** - how to combine all above? The suggested way is to implement an orchestrator class (here called Pipeline) which takes all the structures above (Readers, stage, code...) and class parameters and then chain them together creating processing flow.

6. **Execution** - done in the Processor class which creates all the instances for blocks 1-4 and then passes them to the orchestrator part (5) and executes the processing. Also it should validate and generally handle parameters and create sparkSession. SparkSession is created in the package object. 

## Tests 
TBD