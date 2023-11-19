package org.sdia.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class MySqlApp {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("MySql application").master("local[*]").getOrCreate();

        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/db");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> df=ss.read().format("jdbc").options(options)
                .option("dbtable","tableName")
                .load();
        df.show();
    }

}
