package com.moussi.record.linkage.dataframes

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


        val scored = donationDF.map( md => (md.scoreMatchData(), md.is_match)).toDF("score", "is_match")

        val crossTabsDF = crossTabs(scored, 4.0)
        crossTabsDF.show(false)
    }


    def crossTabs(scored: DataFrame, t: Double) = {

        scored.selectExpr(s"score >= $t as above", "is_match")
            .groupBy("above")
            .pivot("is_match", Seq("true", "false"))
            .count()
    }
}
