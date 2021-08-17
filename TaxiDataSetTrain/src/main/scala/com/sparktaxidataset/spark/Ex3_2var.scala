package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex3_2var {
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

    // получаем максимальное количество пассажиров
    val maxPasCount = df.agg(max("passenger_count")).collect()(0).getInt(0)

    // формируем последовательность из строк, содержащих окончания
    // для названий столбцов от null, 1 до максимального значения количества пассажиров
    var colslist = Seq[String]()
    colslist = colslist :+ "null"
    for(i <- 0 to maxPasCount) {
      colslist = colslist :+ i.toString
    }

    //разворачиваем столбцы с количеством поездок, максимумом и минимумом в строку
    // по соотвествию количеству пассажиров
    val tab = tabCountPas.select(
      Seq(tabCountPas("date"), tabCountPas("tripsCount"), tabCountPas("passenger_count"),
        tabCountPas("max"), tabCountPas("min")) ++:
      colslist.flatMap(c => {
        if(c == "null")
          Seq(
            when(col("passenger_count").isNull, col("tripsCount")).otherwise(0).as("pnull"),
            when(col("passenger_count").isNull, col("max")).otherwise(0).as("maxPnull"),
            when(col("passenger_count").isNull, col("min")).otherwise(0).as("minPnull"))
        else
          Seq(
            when(col("passenger_count") === c.toInt, col("tripsCount")).otherwise(0).as("p" + c),
            when(col("passenger_count") === c.toInt, col("max")).otherwise(0).as("maxP" + c),
            when(col("passenger_count") === c.toInt, col("min")).otherwise(0).as("minP" + c)
          )
      }):_*)

    // создаем последовательность столбцов для расчета сумм
    val sums = colslist.flatMap(colName => Seq(sum("p" + colName).as("p" + colName),
      sum("maxP" + colName).as("maxP" + colName),
      sum("minP" + colName).as("minP" + colName)))

    //схлопываем повернутые строки
    val tab2 = tab.groupBy("date").agg(sums.head, sums.tail:_*)

    //рассчитываем сумму всех поездок
    val fullsum = tab2.select(colslist.map(colName => col("p" + colName)): _*)
          .columns
          .map(c => col(c))
          .reduce((c1,c2) => c1 + c2)

    //рассчитываем отношение количества поездок каждой группы
    //к общему количеству поездок в день и записываем в паркет
    val persentages = tab2.select(
      col("date") +:
      colslist.flatMap(c => Seq(round((col("p" + c) / fullsum * 100).cast("double"), 2).as("persentage_" + c + "p"),
        col("maxP" + c),
        col("minP" + c))):_*)
      .orderBy("date")
      .repartition(2).write.parquet("src/main/tmp/result.parquet")
  }
}
