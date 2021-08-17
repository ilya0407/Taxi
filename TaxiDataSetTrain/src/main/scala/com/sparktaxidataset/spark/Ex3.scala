package com.sparktaxidataset.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex3 {
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
    tabCountPas.createOrReplaceTempView("tabCountPas")

    // получаем максимальное количество пассажиров
    val maxPasCount = df.agg(max("passenger_count")).collect()(0).getInt(0)

    // создаем строку с запросом и добавляем начальные поля в select
    var query = """select date, tripsCount, """ +
      """if(passenger_count is null, tripsCount, 0) as pnull, """ +
      """if(passenger_count is null, max, 0) as maxPnull, """ +
      """if(passenger_count is null, min, 0) as minPnull, """

    // добавляем к полям инструкции для разворота столбцов с с количеством поездок,
    // максимумом и минимумом в строку по соотвествию количеству пассажиров
    for(i <- 0 to maxPasCount)
      query = query + """if(passenger_count = """ + i.toString +""", tripsCount, 0) as p""" + i.toString + """, """ +
                      """if(passenger_count = """ + i.toString +""", max, 0) as maxP""" + i.toString + """, """ +
                      """if(passenger_count = """ + i.toString +""", min, 0) as minP""" + i.toString + """, """
    query = query.dropRight(2)
    query += " from tabCountPas"

    // выполняем запрос
    val tab = spark.sql(query)
    tab.createOrReplaceTempView("tab")

    // аналогично создаем запрос для схлопывания
    var query2 = """select date, sum(pnull) as pnull, sum(maxPnull) as maxPnull, sum(minPnull) as minPnull, """
    for(i <- 0 to maxPasCount)
      query2 = query2 + """sum(p""" + i.toString + """) as p""" + i.toString + """,
          |sum(maxP""".stripMargin + i.toString + """) as maxP""" + i.toString +""",
          |sum(minP""".stripMargin + i.toString + """) as minP""" + i.toString + """, """
    query2 = query2.dropRight(2)
    query2 = query2 + """ from tab """
    query2 = query2 + "group by date"

    // выполняем запрос
    val tab2 = spark.sql(query2)
    tab2.createOrReplaceTempView("tab2")

    // создаем запрос для подсчета суммы поездок
    var fullsum = """(pnull + """
    for(i <- 0 to maxPasCount)
      fullsum = fullsum + """p""" + i.toString + """ + """
    fullsum = fullsum.dropRight(3)
    fullsum = fullsum + """)"""

    // создаем запрос для расчета отношения количества поездок
    // каждой группы к общему количеству поездок в день
    var query3 = """select date, cast(pnull / """ + fullsum +
      """ * 100 as decimal(6,2)) as percentage_null,
         maxPnull, minPnull,
        |""".stripMargin

    for(i <- 0 to maxPasCount)
      query3 = query3 + """cast(p""" + i.toString + """ / """ + fullsum +
                        """ * 100 as decimal(6,2)) as percentage_""" + i.toString +
        """p, maxP""" + i.toString + """, minP""" + i.toString +
        """, """

    query3 = query3.dropRight(2)
    query3 = query3 + """ from tab2"""

    // выполняем запрос и записываем результат в паркет
    val res = spark.sql(query3)
    res.createOrReplaceTempView("res")
    res.repartition(2).write.parquet("src/main/tmp/result.parquet")
  }
}
