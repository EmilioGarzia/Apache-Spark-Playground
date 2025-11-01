/*
* Transformation process Apache Spark exercise
*
*   Request:
*     - Read composed dataset "deezer_mood" stored as parqut in the ExtractionAsDS job
*     - Join deezer composed dataset with another dataset named "tcc_ceds" on "track_name" and "artist_name" columns
*     - Apply the following transformations
*       # replace artist_name value of "gigi d'agostino", "bob marley", "pixies" with their respective hash code
*       # Delete Katy Perry, Taylor Swift and Dua Lipa rows
*       # Aggregate artists by mean of valence, arousal, dating, violence, life
*     - Print artist names relative to the max and min of the calculated means in the preious step
*     - Store this new dataset as Parquet and CSV
* @project_name: "Corn Flakes"
* @author: Emilio Garzia
* @year: 2025
*/

package com.cornflakes.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Transformation {

  // TCC CEDS MUSIC dataset schema definition
  case class tcc_ceds_schema(
                              id: Int,
                              artist_name: String,
                              track_name: String,
                              release_date: Int,
                              genre: String,
                              lyrics: String,
                              len: Int,
                              dating: Double,
                              violence: Double,
                              world_life: Double,
                              night_time: Double,
                              shake_the_audience: Double,
                              family_gospel: Double,
                              romantic: Double,
                              communication: Double,
                              obscene: Double,
                              music: Double,
                              movement_places: Double,
                              light_visual_perceptions: Double,
                              family_spiritual: Double,
                              like_girls: Double,
                              sadness: Double,
                              feelings: Double,
                              danceability: Double,
                              loudness: Double,
                              acousticness: Double,
                              instrumentalness: Double,
                              tcc_valence: Double,
                              energy: Double,
                              topic: String,
                              age: Double
                            )

  // Driver code
  def main(args: Array[String]): Unit = {

    // Spark session initialization
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark_session = SparkSession
      .builder
      .appName("CornFlakes [Transformation phase]")
      .master("local[*]")
      .getOrCreate()

    val deezer_dataset = spark_session.read
      .parquet("exported_data/parquet/deeze_mood_full_dataset.parquet")

    import spark_session.implicits._
    // Originally the dataset has illegal column names (e.g. "movement/places")
    // so we have to rename those columns with allowed names
    val raw_tcc_ceds_dataset = spark_session
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/tcc_ceds_music_1950_2019/tcc_ceds_music.csv")

    // Rename all columns in order to get a cleaned dataset
    val cleaned_tcc_ceds_dataset = raw_tcc_ceds_dataset
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("world/life", "world_life")
      .withColumnRenamed("night/time", "night_time")
      .withColumnRenamed("shake the audience", "shake_the_audience")
      .withColumnRenamed("family/gospel", "family_gospel")
      .withColumnRenamed("movement/places", "movement_places")
      .withColumnRenamed("light/visual perceptions", "light_visual_perceptions")
      .withColumnRenamed("family/spiritual", "family_spiritual")
      .withColumnRenamed("like/girls", "like_girls")
      .withColumnRenamed("valence", "tcc_valence")
      .as[tcc_ceds_schema]

    //    // codeblock useful to check the conversion's result
    //    cleaned_tcc_ceds_dataset.printSchema()
    //    cleaned_tcc_ceds_dataset.createOrReplaceTempView("song")
    //    val query_results = spark_session.sql("SELECT * FROM song WHERE age=1.0")
    //    val results = query_results.collect()
    //    results.foreach(println)

    // Outer join between two dataset
    val joinedDS = cleaned_tcc_ceds_dataset.join(deezer_dataset,
      Seq("track_name", "artist_name"),
      "outer"
    )

    //    // Print some insights about joinedDS
    //    println("\njoinedDS Schema and first 20 rows:")
    //    joinedDS.printSchema()
    //    joinedDS.show()

    // Joined Dataset storage
    joinedDS.coalesce(1).write
      .mode("overwrite")
      .parquet("exported_data/parquet/deezer_tcc_composed_DS.parquet")

    joinedDS.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv("exported_data/csv/deezer_tcc_composed_DS.csv")

    // Substitution: replace three different artists name with their respective hash code
    val dataset_substitution_transformation = joinedDS.withColumn(
      "artist_name",
      when(col("artist_name") === "gigi d'agostino", hash(col("artist_name")).cast("string"))
        .when(col("artist_name") === "bob marley & the wailers", hash(col("artist_name")).cast("string"))
        .when(col("artist_name") === "pixies", hash(col("artist_name")).cast("string"))
        .otherwise(col("artist_name"))
    )

    // Deletion: Delete some artist rows
    val toRemove = Seq("dua lipa", "katy perry", "taylor swift")
    val dataset_deletion_transformation = dataset_substitution_transformation.filter(
      !col("artist_name").isin(toRemove: _*)
    )

    // Aggregation: aggregate using mean about some specific columns
    // filtering in order to remove null values from the computation
    val filtered_dataset = dataset_deletion_transformation
      .filter(
        col("tcc_valence").isNotNull &&
          col("energy").isNotNull &&
          col("dating").isNotNull &&
          col("violence").isNotNull &&
          col("world_life").isNotNull
      )

    val dataset_aggregation_tranformation = filtered_dataset
      .groupBy("artist_name")
      .agg(
        avg("tcc_valence").as("avg_valence"),
        avg("energy").as("avg_arousal"),
        avg("dating").as("avg_dating"),
        avg("violence").as("avg_violence"),
        avg("world_life").as("avg_life")
      )

    // Print scores
    printMaxMin(dataset_aggregation_tranformation, "avg_valence", 5)
    printMaxMin(dataset_aggregation_tranformation, "avg_arousal", 5)
    printMaxMin(dataset_aggregation_tranformation, "avg_dating", 5)
    printMaxMin(dataset_aggregation_tranformation, "avg_violence", 5)
    printMaxMin(dataset_aggregation_tranformation, "avg_life", 5)

    // Store transformed dataset as Parquet and CSV
    dataset_aggregation_tranformation.coalesce(1).write
      .mode("overwrite")
      .csv("exported_data/parquet/deezer_tcc_composed_DS_transformed.parquet")

    dataset_aggregation_tranformation.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv("exported_data/csv/deezer_tcc_composed_DS_transformed.csv")


    spark_session.stop()
  }

  // Support routine to print the artist with the max/min value of a given column
  def printMaxMin(df: org.apache.spark.sql.DataFrame, colName: String, n_results: Int): Unit = {
    println(s"\nMax $colName:")
    df.orderBy(col(colName).desc).select("artist_name", colName).show(n_results, truncate = false)

    println(s"\nMin $colName:")
    df.orderBy(col(colName).asc).select("artist_name", colName).show(n_results, truncate = false)
  }

}
