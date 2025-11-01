<!-- toc start: 3 [do not erase this comment] -->
**Table of contents**
- [Apache Spark playground](#apache-spark-playground)
- [Description](#description)
	- [Extraction](#extraction)
	- [Transformation](#transformation)
- [Dependencies](#dependencies)
- [How to install](#how-to-install)
- [How to run](#how-to-run)
- [Windows command for extraction job](#windows-command-for-extraction-job)
- [Used technologies](#used-technologies)
- [Author](#author)
<!-- toc end [do not erase this comment] -->

# Apache Spark playground

This repo contains a project implemented using `Apache Spark` and `Scala` as programming language.

Basically, I have experienced with **Extraction** and **Transformation** phase from ETL process on big datasets.

# Description

Data pipeline using `Apache Spark` on `Deezer mood detection dataset` and `Kaggle music dataset`

## Extraction

- Download csv dataset [deezer mood dataset](https://github.com/deezer/deezer_mood_detection_dataset)
- Unify the three csv files (train, validation, test) in only one dataset
- Apply strong typing on Dataset (no RDD, no DataFrame)
- Normalize `artist_name` and track_name columns to lowercase
- Store the full dataset on FS using Parquet format
- Export it as `JAR` in order to submit this source as spark job 

## Transformation

- Read composed dataset `deezer_mood` stored as parqut in the `ExtractionAsDS` job
- Join deezer composed dataset with another dataset from Kaggle, named [Tcc Ceds Music](https://www.kaggle.com/datasets/saurabhshahane/music-dataset-1950-to-2019), the join operation must be done on `track_name` and `artist_name` columns
- Apply the following transformations
    * Delete Katy Perry, Taylor Swift and Dua Lipa rows
    * Aggregate artists by mean of valence, arousal, dating, violence, life
    * Print artist names relative to the max and min of the calculated means in the preious step
- Store this new dataset as `Parquet` and `CSV`. All output data are stored in a directory named `exported_data`

# Dependencies

- `Java` ver. `17.0.11`
- `Apache Spark` ver. `3.5.6`
- `sbt` ver. `1.11.5`
- `Hadoop` ver. `3.3.x`
- `Scala` ver. `2.12.20`

ℹ️: Anyway, all those dependencies are contained in a specific directory in the repo named `dependencies` (except for Java SDK and Scala), I suggest to move them on simple path, i.e. `C:\` (on Windows machines).

# How to install

In order to use hadoop file system on Windows, but also to use work on local file system, we have to download `hadoop` directory which contains `winutils.exe` and `hadoop.dll`. Without this directory we are not able to write anything on our file system.

* Add user variable `HADOOP_HOME` with value `path\hadoop`
* Add `hadoop\bin` to env var `PATH`

I suggest to add also `spark`, `sbt`, `java` to env vars (but is not required).

# How to run

Basically, the actual implementation of the project is whole contained in two source files:

* `ExtractionAsDS.scala` which perform data extraction as DataSet object (no RDD, no Dataframe)
* `Transformation.scala` which perform data transformation

Both job can be executed directly from IDE (`intelliJ`), but I have exported them as JAR files, in this way you can launch those scripts directly on `spark` simulating an actual execution on a cluster (even if you are on local machine).

In order to do this:

```shell
# Windows command for extraction job
C:\Spark\bin\spark-submit path_project\out\artifacts\ExtractionAsDS\ExtractionAsDS.jar
```

# Used technologies

- `Apache Spark`
- `Scala` (as programming language)
- `Jetbrains IntelliJ` (as IDE)

# Author

*Emilio Garzia, 2025*
