package org.sdia.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySqlApp {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("MySql application").master("local[*]").getOrCreate();

        //Dataset<Row> df=ss.read().format("jdbc").option("driver","com.mysql.jdbc.");
    }

}
