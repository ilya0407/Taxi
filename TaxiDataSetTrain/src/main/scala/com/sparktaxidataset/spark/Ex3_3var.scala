package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex3_3var {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]")
      .appName("SparkTaxiDataset")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filePath = "src/main/resources/yellow_tripdata_2020-01.csv"

    val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ","))
      .csv(filePath)
      .withColumn(
        "date",
        to_date(
          col("tpep_pickup_datetime"),
          "yyyy-mm-dd hh:mm:ss"))
      .filter(col("date").between("2020-01-01","2020-01-31"))

    val maxPasCount = df.agg(max("passenger_count")).collect()(0).getInt(0)

    var colslist = Seq[String]()
    colslist = colslist :+ "null"
    for(i <- 0 to maxPasCount) {
      colslist = colslist :+ i.toString
    }

    val counts = colslist.flatMap(c => {
      if(c == "null") {
        Seq(
        round(count(when(col("passenger_count").isNull, 1)) / sum("passenger_count") * 100, 2)
          .as("percentage_" + c),
        max(when(col("passenger_count").isNull, col("total_amount")))
          .as("maxP" + c),
        min(when(col("passenger_count").isNull, col("total_amount")))
          .as("minP" + c),
        )
      } else {
        Seq(
        round(count(when(col("passenger_count") === c.toInt, 1)) / sum("passenger_count") * 100, 2)
          .as("percentage_" + c + "p"),
        max(when(col("passenger_count") === c.toInt, col("total_amount")))
          .as("maxP" + c),
        min(when(col("passenger_count") === c.toInt, col("total_amount")))
          .as("minP" + c),
        )
      }
    })

    val tabCountPas = df
      .groupBy("date")
      .agg(
        counts.head, counts.tail:_*
      ).orderBy("date")

    tabCountPas.show()
    Thread.sleep(Int.MaxValue)
  }
}
