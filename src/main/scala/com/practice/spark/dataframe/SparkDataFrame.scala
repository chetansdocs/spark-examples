package com.practice.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object SparkDataFrame {
  /**
   * create dataframe from json file
   * @param sparkSession Spark Session
   * @return [[DataFrame]]
   */
  def readJson(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("multiline", "true").json("src/main/resources/StudentJson.json")
  }

  /**
   * When otherwise example
   * @param inputDf Input Dataframe
   * @return [[DataFrame]]
   */
  def whenOtherWise(inputDf: DataFrame): DataFrame = {
    val classDetails = inputDf.withColumnRenamed("Grade ", "Grade")
      .withColumn("Class", when(col("Grade") >= 75, "Distinction")
        .when(col("Grade") >= 60, "First Class")
        .when(col("Grade") >= 50, "Second Class")
        .otherwise("Fail")
      )
    classDetails.show()
    classDetails
  }

  /**
   * Display gender wise Grade
   * @param inputDf Input DataFrame
   */
  def pivotTable(inputDf: DataFrame): Unit = {
    val columns = Seq("Gender", "Class")
    val selectDf = inputDf.select(columns.map(col):_*)
    val pivotDf = selectDf.groupBy("Gender").pivot("Class").count()
    pivotDf.show()
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val inputDf = readJson
    val classDf = whenOtherWise(inputDf)
    pivotTable(classDf)
  }
}
