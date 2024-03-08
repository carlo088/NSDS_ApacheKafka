package it.polimi.middleware.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType; 

/**
 * SparkAnalysis
 *
 * Input: a csv files with schema:
 *     (groupID,
 *      listOfPeople,
 *      nationality,
 *      age,
 *      dateJoined,
 *      lifetime,
 *      currentCardinality,
 *      minCardinality,
 *      maxCardinality,
 *      averageCardinality,
 *      nChangeCardinality)
 * 
 * 
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
        schemaFields.add(DataTypes.createStructField("groupID", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("listOfPeople", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("nationality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("dateJoined", DataTypes.DateType, true));
        schemaFields.add(DataTypes.createStructField("lifetime", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("currentCardinality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("minCardinality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("maxCardinality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("averageCardinality", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("nChangeCardinality", DataTypes.StringType, true));
        final StructType schema = DataTypes.createStructType(schemaFields);

        final Dataset<Row> data = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "dd/MM/yy")
                .schema(schema)
                .csv(filePath + "data/output.csv");
        System.out.println(" ");
        System.out.println("Original data:");
        data.show();

        // Transform data
        Dataset<Row> deviceID_df = data.select(explode(split(col("listOfPeople"), ",")).alias("DeviceID")).withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> dateJoined_df = data.withColumn("DeviceID", explode(split(col("listOfPeople"), ","))).select(col("dateJoined")).withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> nationality_df = data.select(explode(split(col("nationality"), ",")).alias("Nationality")).withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> age_df = data.select(explode(split(col("age"), ",")).alias("Age")).withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> firstJoin = deviceID_df.join(dateJoined_df, "id");
        Dataset<Row> secondJoin = firstJoin.join(nationality_df, "id");
        Dataset<Row> dataSpark = secondJoin.join(age_df, "id").drop("id");
        System.out.println("Data transformed for Spark SQL queries:");
        dataSpark.show();

        System.out.println("Starting SQL queries...");
        System.out.println(" ");
        Dataset<Row> distinctDates = dataSpark.select(col("dateJoined")).distinct();
        List<String> datesList = distinctDates.as(Encoders.STRING()).collectAsList();

        for (String date : datesList) {

            //////////////////////////////////////////////////////////////////////
            // Q1. A 7 days moving average of participants’ age per nationality, 
            // computed for each day. For a given nationality, consider all the
            // groups in which at least one participant is of that nationality
            //////////////////////////////////////////////////////////////////////

            Dataset<Row> filteredGroups = dataSpark
                        .filter(expr("dateJoined >= date_sub('" + date + "', 6)"))
                        .filter(expr("dateJoined <= '" + date + "'"));

            Dataset<Row> movingAverage = filteredGroups
                            .groupBy("Nationality")
                            .agg(round(avg("Age"), 2).as("movingAverage"))
                            .withColumn("StartDate", lit(date))
                            .withColumn("EndDate", lit(java.time.LocalDate.parse(date).minusDays(6).toString()))
                            .orderBy("Nationality");
            
            System.out.println("Moving average for date " + date + ":");
            movingAverage.show();

            //////////////////////////////////////////////////////////////////////
            // Q2. Percentage increase (with respect to the day before) 
            // of the 7 days moving average, for each day
            //////////////////////////////////////////////////////////////////////

            String dayBefore = java.time.LocalDate.parse(date).minusDays(1).toString();
            
            Dataset<Row> filteredGroupsDayBefore = dataSpark
                .filter(expr("dateJoined >= date_sub('" + dayBefore + "', 6)"))
                .filter(expr("dateJoined <= '" + dayBefore + "'"));
        
            // Calculate day's before moving average
            Dataset<Row> movingAverageDayBefore = filteredGroupsDayBefore
                .groupBy("Nationality")
                .agg(round(avg("Age"), 2).as("movingAverageDayBefore")).orderBy("Nationality");
        
            // Join yesterday's and today's moving averages to compute percentage increase
            Dataset<Row> percentageIncrease = movingAverageDayBefore
                .join(movingAverage, "Nationality")
                .withColumn("PercentageIncrease",
                            round((col("movingAverage").divide(col("movingAverageDayBefore")).minus(1)).multiply(100), 2))
                .withColumn("Date", lit(date))
                .orderBy("Nationality")
                .drop("StartDate", "EndDate");

            // Print only if the DataFrame is not empty
            if (percentageIncrease.count() > 0) {
                System.out.println("Percentage increase for date " + date + ":");
                percentageIncrease.show();

                //////////////////////////////////////////////////////////////////////
                // Q3. Top nationalities with the highest percentage increase 
                // of the seven days moving average, for each day
                //////////////////////////////////////////////////////////////////////

                WindowSpec windowSpec = Window.partitionBy("Date").orderBy(desc("PercentageIncrease"));

                Dataset<Row> rankedNationalities = percentageIncrease
                        .withColumn("rank", rank().over(windowSpec))
                        .filter(col("rank").leq(1)) // Keep only top 1 nationalities
                        .drop("rank")
                        .select("Date", "Nationality", "PercentageIncrease")
                        .orderBy(col("Date"), desc("PercentageIncrease"));

                System.out.println("Top nationalities with highest percentage increase for date " + date + ":");
                rankedNationalities.show();

            } else {
                System.out.println("Percentage increase DataFrame for date " + date + " is empty.");
                System.out.println("As percentage increase is empty, can't compute query 3 for date " + date);
            }
        }

        spark.close();

    }
}
