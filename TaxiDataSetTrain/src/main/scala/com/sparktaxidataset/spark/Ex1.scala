package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Ex1 {
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

    //группируем по дате и количеству пассажиров, далее считаем количество поездок для каждой группы
    val tabCountPas = df.groupBy("date", "passenger_count")
      .agg(count("*").as("tripsCount"))
    tabCountPas.show()

    //разворачиваем столбец с количеством поездок в строку по соотвествию количеству пассажиров
    val tab = tabCountPas.select(col("date"),
      col("tripsCount"),
      expr("if(passenger_count is null, -1, 0)").as("pnull"),
      expr("if(passenger_count = 0, tripsCount, 0)").as("p0"),
      expr("if(passenger_count = 1, tripsCount, 0)").as("p1"),
      expr("if(passenger_count = 2, tripsCount, 0)").as("p2"),
      expr("if(passenger_count = 3, tripsCount, 0)").as("p3"),
      expr("if(passenger_count > 3, tripsCount, 0)").as("pg3"))
    //схлопываем повернутые строки
    val tab2 = tab
      .groupBy("date")
      .agg(
        sum("pnull").as("pnull"),
        sum("p0").as("p0"),
        sum("p1").as("p1"),
        sum("p2").as("p2"),
        sum("p3").as("p3"),
        sum("pg3").as("pg3")
      )
    //рассчитываем отношение количества поездок каждой группы к общему количеству поездок в день и записываем в паркет
    val res = tab2.select(col("date"),
      round(expr("pnull / (pnull + p0 + p1 + p2 + p3+ pg3)").multiply(100).cast("double"),2).as("percentage_null"),
      round(expr("p0 / (pnull + p0 + p1 + p2 + p3 + pg3)").multiply(100).cast("double"),2).as("percentage_zero"),
      round(expr("p1 / (pnull + p0 + p1 + p2 + p3 + pg3)").multiply(100).cast("double"),2).as("percentage_1p"),
      round(expr("p2 / (pnull + p0 + p1 + p2 + p3 + pg3)").multiply(100).cast("double"),2).as("percentage_2p"),
      round(expr("p3 / (pnull + p0 + p1 + p2 + p3 + pg3)").multiply(100).cast("double"),2).as("percentage_3p"),
      round(expr("pg3 / (pnull + p0 + p1 + p2 + p3 + pg3)").multiply(100).cast("double"),2).as("percentage_4p_plus"))
      .orderBy("date")
      .repartition(2).write.parquet("src/main/tmp/result.parquet")
  }
}
