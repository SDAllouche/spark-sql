package org.sdia.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvApp {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("csv application").master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("header",true).option("inferSchema",true).csv("products.csv");
        // df.show();
        df.printSchema();
        //df.select(col("price").plus(2000)).show();
    }

}
