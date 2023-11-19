package org.sdia.hospital;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class HospitalManagement {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("MySql application").master("local[*]").getOrCreate();

        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/db_hopital");
        options.put("user","root");
        options.put("password","");

        //Doctors Table
        Dataset<Row> doctors=ss.read().format("jdbc").options(options)
                .option("dbtable","medecins")
                .load();
        //doctors.show();

        //Patients Table
        Dataset<Row> patients=ss.read().format("jdbc").options(options)
                .option("dbtable","patients")
                .load();
        //patients.show();

        //Consultation Table
        Dataset<Row> consults=ss.read().format("jdbc").options(options)
                .option("dbtable","consultations")
                .load();
        //consults.show();

        //Consultations By day
        Dataset<Row> consultsWithDay=consults.withColumn("day",dayofmonth(col("date")));
        Dataset<Row> consultsPerDay = consultsWithDay.groupBy("day")
                .agg(count("*").alias("consultsPerDay"))
                .orderBy("day");
        //consultsPerDay.show();

        //Consultations By date
        Dataset<Row> consultsPerDate = consultsWithDay.groupBy("date")
                .agg(count("*").alias("consultsCount"))
                .orderBy(desc("date"));
        consultsPerDate.show();

    }
}
