package com.moussi.dataframes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecordLinkageModeling {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession.builder().appName("SparkPivotingAndReshaping").master("local[*]").getOrCreate()

        import ss.implicits._
        val donationDF = ss.read
            .option("header", "true")
            .option("nullValue", "?")
            .option("inferSchema", "true")
            .csv(
            "/home/amoussi/Desktop/learning/spark_analytics/data/donation").as[MatchData]



    }
}
