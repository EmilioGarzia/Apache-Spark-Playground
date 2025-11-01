/*
* Extraction process Apache Spark exercise
*
*   Request:
*     - Download csv dataset "deezer mood dataset"
*     - Unify the three csv files (train, validation, test) in only one dataset
*     - Apply strong typing on Dataset (no RDD, no DataFrame)
*     - Normalize artist_name and track_name columns to lowercase
*     - Store the full dataset on FS using Parquet format
*     - Export it as JAR in order to submit this source as spark job
*
* @project_name: "Corn Flakes"
* @author: Emilio Garzia
* @year: 2025
*/

package com.cornflakes.spark

import org.apache.log4j._
import org.apache.spark.sql.{Dataset, SparkSession}

object ExtractionAsDS {

  // deezer mood dataset schema definition
  case class deezer_mood_schema(
                   dzr_sng_id:Int,
                   MSD_sng_id:String,
                   MSD_track_id:String,
                   deezer_valence:Double,
                   arousal:Double,
                   artist_name:String,
                   track_name:String
                 )

  def main(args: Array[String]): Unit = {

    // Spark session initialization
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark_session = SparkSession
      .builder
      .appName("CornFlakes [Extraction as DataFrame]")
      .master("local[*]")
      .getOrCreate()

    import spark_session.implicits._

    // load datasets from csv
    def load_songs(data_path: String): Dataset[deezer_mood_schema] = {
      spark_session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(data_path)
        .withColumnRenamed("valence", "deezer_valence")
        .as[deezer_mood_schema]

    }

    // Load datasets and print info about them
    val test_set = load_songs("data/deezer_mood_detection_dataset-master/test.csv")
    val train_set = load_songs("data/deezer_mood_detection_dataset-master/train.csv")
    val validation_set = load_songs("data/deezer_mood_detection_dataset-master/validation.csv")
    val full_dataset_raw = test_set.union(train_set).union(validation_set)

    // normalize string columns to lower-case in order to compare this DS with tcc_ceds DS
    val full_dataset = full_dataset_raw.map(row =>
      row.copy(
        artist_name = row.artist_name.toLowerCase,
        track_name = row.track_name.toLowerCase
      )
    )

    println("Dataset insights:")
    println("Total in test: " + test_set.count())
    println("Total in train: " + train_set.count())
    println("Total in validation: " + validation_set.count())
    println("Total in full dataset: " + full_dataset.count() + "\n")

    // Query output test on full dataset
    full_dataset.printSchema()
    full_dataset.createOrReplaceTempView("song")
    val query_results = spark_session.sql("SELECT * FROM song WHERE artist_name='britney spears' OR artist_name='fiona apple'")
    val results = query_results.collect()
    results.foreach(println)
    println("\nBritney Spears and Fiona Apple total songs: " + query_results.count())

    // Store as Parquet file
    full_dataset.coalesce(1).write
      .mode("overwrite")
      .parquet("exported_data/parquet/deeze_mood_full_dataset.parquet")

    full_dataset.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv("exported_data/csv/deeze_mood_full_dataset.csv")

    spark_session.stop()
  }
}
