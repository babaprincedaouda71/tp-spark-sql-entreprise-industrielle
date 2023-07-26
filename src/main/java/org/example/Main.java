package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class Main {
    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("TP Spark SQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfIncidents = sparkSession.read()
                .option("multiline", "true")
                .csv("incidents.csv");

        dfIncidents.createTempView("Incidents");

        // Nombre d'incident par Service
        sparkSession.sql("select _c3 as Service, count(*) as Nombre_incidents from Incidents group by _c3").show();

        // Années avec le plus d'incidents*
        dfIncidents = dfIncidents.withColumn("_c4", year(col("_c4")));
        dfIncidents.groupBy("_c4").count().orderBy(desc("count")).limit(2).show();
    }
}