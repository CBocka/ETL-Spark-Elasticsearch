import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, when, year, month, dayofmonth, lit}
import org.elasticsearch.spark.sql.sparkDatasetFunctions

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .config("spark.es.nodes.wan.only", "true")
      .config("spark.es.net.ssl", "false")
      .appName("ETL Spark Elasticsearch")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Seq(
      StructField("player_id", StringType),
      StructField("fifa_version", LongType),
      StructField("short_name", StringType),
      StructField("long_name", StringType),
      StructField("player_positions", StringType),
      StructField("overall", LongType),
      StructField("value_eur", DoubleType),
      StructField("dob", StringType),
      StructField("height_cm", LongType),
      StructField("weight_kg", LongType),
      StructField("league_name", StringType),
      StructField("club_name", StringType),
      StructField("club_joined_date", StringType),
      StructField("nationality_name", StringType),
      StructField("preferred_foot", StringType),
      StructField("player_tags", StringType),
      StructField("player_face_url", StringType),
    ))

    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("fifa_players.csv")

    val df23 = df
      .where(col("fifa_version") === 23)
      .withColumn("player_age",
        year(lit("2023-09-1")) - year(col("dob")) -
        (when(month(lit("2023-09-1")) < month(col("dob")) ||
          (month(lit("2023-09-1")) === month(col("dob")) && dayofmonth(lit("2023-09-1")) < dayofmonth(col("dob"))), 1)
          .otherwise(0)))

    val df22 = df
      .where(col("fifa_version") === 22)
      .withColumn("player_age",
        year(lit("2022-09-1")) - year(col("dob")) -
          (when(month(lit("2022-09-1")) < month(col("dob")) ||
            (month(lit("2022-09-1")) === month(col("dob")) && dayofmonth(lit("2022-09-1")) < dayofmonth(col("dob"))), 1)
            .otherwise(0)))

    val df21 = df
      .where(col("fifa_version") === 21)
      .withColumn("player_age",
        year(lit("2021-09-1")) - year(col("dob")) -
          (when(month(lit("2021-09-1")) < month(col("dob")) ||
            (month(lit("2021-09-1")) === month(col("dob")) && dayofmonth(lit("2021-09-1")) < dayofmonth(col("dob"))), 1)
            .otherwise(0)))


    // Guardar los DataFrames en indices de Elasticsearch
    df23.saveToEs("fifa_players_23_index")
    df22.saveToEs("fifa_players_22_index")
    df21.saveToEs("fifa_players_21_index")

    spark.stop()
  }
}