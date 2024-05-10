package it.polimi.middleware.spark;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType; 

/**
 * SparkAnalysis
 *
 * Input: a csv files with schema:
 *     (date,
 *      nationality,
 *      age)
 *
 * Queries
 * Q1. A 7 days moving average of participants’ age per nationality, computed for each day
 *      For a given nationality, consider all the groups in which at least one participant is of that nationality
 * Q2. Percentage increase (with respect to the day before) of the 7 days moving average, for each day
 * Q3. Top nationalities with the highest percentage increase of the seven days moving average, for each day
 */

public class SparkAnalysis {
    private static final boolean useCache = true;

    public static void main(String[] args) {

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "SparkAnalysisWithCache" : "SparkAnalysisNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // declare structure of DataFrame
        final List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("date", DataTypes.DateType, true));
        schemaFields.add(DataTypes.createStructField("nationality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        final StructType schema = DataTypes.createStructType(schemaFields);

        final Dataset<Row> dataNodeRed = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "yyyy-MM-dd")
                .schema(schema)
                .csv(filePath + "data/to_spark.csv");

        // System.out.println(" ");
        // System.out.println("Original data:");
        // dataNodeRed.show();

        // Make it so that data only has 1 row, per nationality, per day
        Dataset<Row> data = dataNodeRed
                .groupBy("date", "nationality")
                .agg(avg("age").as("age"))
                .orderBy("date");

        System.out.println(System.lineSeparator() + "Starting SQL queries..." + System.lineSeparator());

        ////////////////////////////////////////////////////////////////////////
        // Q1. A 7 days moving average of participants’ age per nationality,  //
        // computed for each day. For a given nationality, consider all the   //
        // groups in which at least one participant is of that nationality    //
        ////////////////////////////////////////////////////////////////////////

        WindowSpec windowSpecMovingAverage = Window
                .partitionBy("nationality")
                .orderBy("date")
                .rowsBetween(-6, 0);

        final Dataset<Row> movingAverage = data
                .withColumn("movingAverage", round(avg("age").over(windowSpecMovingAverage), 2))
                .drop("age")
                .orderBy("date", "nationality");

        movingAverage.show();


        ////////////////////////////////////////////////////////////////
        // Q2. Percentage increase (with respect to the day before)   //
        // of the 7 days moving average, for each day                 //
        ////////////////////////////////////////////////////////////////

        WindowSpec windowSpecPercentageIncrease = Window
                .partitionBy("nationality")
                .orderBy("date");

        Dataset<Row> percentageIncrease = movingAverage
                .withColumn("previous", lag("movingAverage", 1).over(windowSpecPercentageIncrease))
                .withColumn("percentageIncrease",
                        when(col("previous").isNotNull(),
                                (col("movingAverage").divide(col("previous")).multiply(100)).minus(100))
                                .otherwise(null))
                .withColumn("percentageIncrease", round(col("percentageIncrease"), 2))
                .drop("previous")
                .drop("movingAverage")
                .orderBy(desc("date"), col("nationality"));
        
        percentageIncrease.show();


        //////////////////////////////////////////////////////////////////
        // Q3. Top nationalities with the highest percentage increase   //
        // of the seven days moving average, for each day               //
        //////////////////////////////////////////////////////////////////

        WindowSpec windowSpec = Window
                .partitionBy("date")
                .orderBy(desc("percentageIncrease"));

        Dataset<Row> rankedNationalities = percentageIncrease
                .withColumn("rank", rank().over(windowSpec))
                .filter(col("rank").leq(2)) // Keep only the top 2 nationalities for each day
                .drop("rank")
                .orderBy("date", "nationality")
                .orderBy(desc("percentageIncrease"));
    
        rankedNationalities.show();

        spark.close();

    }
}
