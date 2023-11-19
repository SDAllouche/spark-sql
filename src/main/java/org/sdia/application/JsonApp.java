package org.sdia.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonApp {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().appName("Products json").master("local").getOrCreate();
        Dataset <Row> ds=ss.read().option("multiline",true).option("inferSchema",true).json("products.json");
        ds.show();
    }
}
