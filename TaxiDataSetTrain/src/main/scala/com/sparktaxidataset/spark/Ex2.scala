package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]")
      .appName("SparkTaxiDataset")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filePath = "src/main/resources/yellow_tripdata_2020-01.csv"

    // читаем файл преобразуем дату и фильтруем значения для января
    val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ","))
      .csv(filePath)
      .withColumn(
        "date",
        to_date(
          col("tpep_pickup_datetime"),
          "yyyy-mm-dd hh:mm:ss"))
      .filter(col("date").between("2020-01-01","2020-01-31"))

    //группируем по дате и количеству пассажиров, далее считаем количество поездок,
    //максимальную и минимальную цену поездки для каждой группы
    val tabCountPas = df
      .groupBy("date", "passenger_count")
      .agg(
        count("*").as("tripsCount"),
        max("total_amount").as("max"),
        min("total_amount").as("min")
      ).orderBy("date","passenger_count")

    //разворачиваем столбцы с количеством поездок, максимумом и минимумом в строку по соотвествию количеству пассажиров
    val tab = tabCountPas.select(col("date"),
        col("tripsCount"),
        expr("if(passenger_count is null, trips_count, 0)").as("pnull"),
        expr("if(passenger_count = 0, tripsCount, 0)").as("p0"),
        expr("if(passenger_count = 1, tripsCount, 0)").as("p1"),
        expr("if(passenger_count = 2, tripsCount, 0)").as("p2"),
        expr("if(passenger_count = 3, tripsCount, 0)").as("p3"),
        expr("if(passenger_count > 3, tripsCount, 0)").as("pg3"),
        expr("if(passenger_count is null, max, 0)").as("maxPnull"),
        expr("if(passenger_count = 0, max, 0)").as("maxP0"),
        expr("if(passenger_count = 1, max, 0)").as("maxP1"),
        expr("if(passenger_count = 2, max, 0)").as("maxP2"),
        expr("if(passenger_count = 3, max, 0)").as("maxP3"),
        expr("if(passenger_count > 3, max, 0)").as("maxPG3"),
        expr("if(passenger_count is null, min, 0)").as("minPnull"),
        expr("if(passenger_count = 0, min, 0)").as("minP0"),
        expr("if(passenger_count = 1, min, 0)").as("minP1"),
        expr("if(passenger_count = 2, min, 0)").as("minP2"),
        expr("if(passenger_count = 3, min, 0)").as("minP3"),
        expr("if(passenger_count > 3, min, 0)").as("minPG3")
      ).orderBy("date")

    //схлопываем повернутые строки
    val tab2 = tab.groupBy("date")
      .agg(
        sum("pnull").as("pnull"),
        sum("p0").as("p0"),
        sum("p1").as("p1"),
        sum("p2").as("p2"),
        sum("p3").as("p3"),
        sum("pg3").as("pg3"),

        sum("maxPnull").as("maxPnull"),
        sum("maxP0").as("maxP0"),
        sum("maxP1").as("maxP1"),
        sum("maxP2").as("maxP2"),
        sum("maxP3").as("maxP3"),
        sum("maxPG3").as("maxPG3"),

        sum("minPnull").as("minPnull"),
        sum("minP0").as("minP0"),
        sum("minP1").as("minP1"),
        sum("minP2").as("minP2"),
        sum("minP3").as("minP3"),
        sum("minPG3").as("minPG3")
      )

    //рассчитываем отношение количества поездок каждой группы к общему количеству поездок в день и записываем в паркет
    val res = tab2.select(col("date"),
      round(expr("pnull / (pnull + p0 + p1 + p2 + p3+ pg3)")
        .multiply(100).cast("double"),2).as("percentage_null"),
      col("maxPnull"),
      col("minPnull"),
      round(expr("p0 / (pnull + p0 + p1 + p2 + p3 + pg3)")
        .multiply(100).cast("double"),2).as("percentage_zero"),
      col("maxP0"),
      col("minP0"),
      round(expr("p1 / (pnull + p0 + p1 + p2 + p3 + pg3)")
        .multiply(100).cast("double"),2).as("percentage_1p"),
      col("maxP1"),
      col("minP1"),
      round(expr("p2 / (pnull + p0 + p1 + p2 + p3 + pg3)")
        .multiply(100).cast("double"),2).as("percentage_2p"),
      col("maxP2"),
      col("minP2"),
      round(expr("p3 / (pnull + p0 + p1 + p2 + p3 + pg3)")
        .multiply(100).cast("double"),2).as("percentage_3p"),
      col("maxP3"),
      col("minP3"),
      round(expr("pg3 / (pnull + p0 + p1 + p2 + p3 + pg3)")
        .multiply(100).cast("double"),2).as("percentage_4p_plus"),
      col("maxPG3"),
      col("minPG3")
    ).orderBy("date")
      .repartition(2).write.parquet("src/main/tmp/result.parquet")
  }
}
