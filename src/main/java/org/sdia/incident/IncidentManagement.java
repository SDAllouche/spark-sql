package org.sdia.incident;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class IncidentManagement {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("Incidents Management").master("local[*]").getOrCreate();
        Dataset<Row> df=ss.read().option("header",true).option("inferSchema",true).csv("incidents.csv");
        df = df.withColumn("date", to_date(col("date"), "dd/MM/yyyy"));
        df.show();

        //Show incidents by service
        Dataset<Row> incidentByService = df.groupBy("service").agg(count("*").alias("incidentByService"));
        incidentByService.show();

        //show 2 years that have the most incidents
        Dataset<Row> df1 = df.withColumn("year", year(col("date")));
        df1.show();

        Dataset<Row> incidentByYear = df1.groupBy("year")
                .agg(count("*").alias("incidentByYear"))
                .orderBy(desc("incidentByYear"))
                .limit(2);

        incidentByYear.show();
    }
}
