package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex2_2var {
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
      )

    //разворачиваем столбцы с количеством поездок, максимумом и минимумом в строку по соотвествию количеству пассажиров
    val tab = tabCountPas.select(col("date"),
      col("tripsCount"),
      when(col("passenger_count").isNull, col("tripsCount")).otherwise(0).as("pnull"),
      when(col("passenger_count").isNull, col("max")).otherwise(0).as("maxPnull"),
      when(col("passenger_count").isNull, col("min")).otherwise(0).as("minPnull"),

      when(col("passenger_count") === 0, col("tripsCount")).otherwise(0).as("p0"),
      when(col("passenger_count") === 0, col("max")).otherwise(0).as("maxP0"),
      when(col("passenger_count") === 0, col("min")).otherwise(0).as("minP0"),

      when(col("passenger_count") === 1, col("tripsCount")).otherwise(0).as("p1"),
      when(col("passenger_count") === 1, col("max")).otherwise(0).as("maxP1"),
      when(col("passenger_count") === 1, col("min")).otherwise(0).as("minP1"),

      when(col("passenger_count") === 2, col("tripsCount")).otherwise(0).as("p2"),
      when(col("passenger_count") === 2, col("max")).otherwise(0).as("maxP2"),
      when(col("passenger_count") === 2, col("min")).otherwise(0).as("minP2"),

      when(col("passenger_count") === 3, col("tripsCount")).otherwise(0).as("p3"),
      when(col("passenger_count") === 3, col("max")).otherwise(0).as("maxP3"),
      when(col("passenger_count") === 3, col("min")).otherwise(0).as("minP3"),

      when(col("passenger_count") > 3, col("tripsCount")).otherwise(0).as("pg3"),
      when(col("passenger_count") > 3, col("max")).otherwise(0).as("maxPG3"),
      when(col("passenger_count") > 3, col("min")).otherwise(0).as("minPG3"),
    )

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

    //рассчитываем сумму всех поездок
    val fullsum = tab2("pnull") + tab2("p0") + tab2("p1") + tab2("p2") + tab2("p3") + tab2("pg3")

    //рассчитываем отношение количества поездок каждой группы
    // к общему количеству поездок в день и записываем в паркет
    val res = tab2.select(col("date"),
      round((tab2("pnull") / fullsum * 100).cast("double"),2).as("percentage_null"),
      col("maxPnull"),
      col("minPnull"),
      round((tab2("p0") / fullsum * 100).cast("double"),2).as("percentage_zero"),
      col("maxP0"),
      col("minP0"),
      round((tab2("p1") / fullsum * 100).cast("double"),2).as("percentage_1p"),
      col("maxP1"),
      col("minP1"),
      round((tab2("p2") / fullsum * 100).cast("double"),2).as("percentage_2p"),
      col("maxP2"),
      col("minP2"),
      round((tab2("p3") / fullsum * 100).cast("double"),2).as("percentage_3p"),
      col("maxP3"),
      col("minP3"),
      round((tab2("pg3") / fullsum * 100).cast("double"),2).as("percentage_4p_plus"),
      col("maxPG3"),
      col("minPG3"))
      .orderBy("date")
      .repartition(2).write.parquet("src/main/tmp/result.parquet")
  }
}
