package com.moussi.record.linkage.dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkPivotingAndReshaping {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession.builder().appName("SparkPivotingAndReshaping").master("local[*]").getOrCreate()

        import ss.implicits._
        val donationDF = ss.read
            .option("header", "true")
            .option("nullValue", "?")
            .option("inferSchema", "true")
            .csv(
            "/home/amoussi/Desktop/learning/spark_analytics/data/donation").as[MatchData]

        val matchSummary = donationDF.filter($"is_match" === true)
        val missSummary = donationDF.filter($"is_match" === false)


        val matchSummaryWideLongForm: DataFrame = pivotDescSummary(matchSummary.describe())
        val missSummaryWideLongForm: DataFrame = pivotDescSummary(missSummary.describe())


        println("matchSummaryWideLongForm **************")
        matchSummaryWideLongForm.show(false)
        println("missSummaryWideLongForm **************")
        missSummaryWideLongForm.show(false)

        val summaryJoinDF = CalculateMeanCountDiffApiBased(matchSummaryWideLongForm, missSummaryWideLongForm)

        summaryJoinDF.printSchema()
        summaryJoinDF.show(false)

        matchSummaryWideLongForm.createOrReplaceTempView("match_desc")
        missSummaryWideLongForm.createOrReplaceTempView("miss_desc")

        val summaryJoinSqlDF = ss.sql(
            """
                        SELECT a.field, a.count + b.count total, a.mean - b.mean delta
                        FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
                        WHERE a.field NOT IN ("id_1", "id_2")
                        ORDER BY delta DESC, total DESC
                        """)
        summaryJoinSqlDF.show(false)

    }

    private def CalculateMeanCountDiffApiBased(matchSummaryWideLongForm: DataFrame,
        missSummaryWideLongForm: DataFrame) = {
        matchSummaryWideLongForm.join(missSummaryWideLongForm, "field")
            .where(matchSummaryWideLongForm("field") =!= "id_2" and matchSummaryWideLongForm("field") =!= "id_1")
            .select(
                $"field",
                matchSummaryWideLongForm("count") + missSummaryWideLongForm("count") as "total",
                matchSummaryWideLongForm("mean") - missSummaryWideLongForm("mean") as "delta")
            .orderBy(desc("delta"), desc("total"))
    }

    private def pivotDescSummary(summary: DataFrame) = {
        import summary.sparkSession.implicits._
        import com.moussi.record.linkage.utils.StringSafeConverter._

        val schema = summary.schema
        val longForm = summary.flatMap(
            row => {
                val metric = row.getString(0)
                (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toSafeDouble))
            })

        val longDF = longForm.toDF("metric", "field", "value")

        val wideForm = longDF
            .groupBy("field")
            .pivot("metric", Seq("count", "mean", "stddev", "min", "max"))
            .agg(first("value"))

        wideForm.select("field", "count", "mean")
    }
}
