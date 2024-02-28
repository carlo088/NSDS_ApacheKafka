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
 * Input: 2 csv files with 
 * 1. people, schema ("DeviceID: Int, Nationality: String, Age: Int)
 * 2. groups, schema ("GroupID: Int, Members: String, Timestamp: TimestampType)
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

        // declare structure of DataFrame people
        final List<StructField> peopleSchemaFields = new ArrayList<>();
        peopleSchemaFields.add(DataTypes.createStructField("DeviceID", DataTypes.IntegerType, true));
        peopleSchemaFields.add(DataTypes.createStructField("Nationality", DataTypes.StringType, true));
        peopleSchemaFields.add(DataTypes.createStructField("Age", DataTypes.IntegerType, true));
        peopleSchemaFields.add(DataTypes.createStructField("DateJoined", DataTypes.DateType, true));
        final StructType peopleSchema = DataTypes.createStructType(peopleSchemaFields);

        // declare structure of DataFrame groups
        // final List<StructField> groupsSchemaFields = new ArrayList<>();
        // groupsSchemaFields.add(DataTypes.createStructField("GroupID", DataTypes.IntegerType, true));
        // groupsSchemaFields.add(DataTypes.createStructField("Members", DataTypes.StringType, true));
        // groupsSchemaFields.add(DataTypes.createStructField("Timestamp", DataTypes.TimestampType, true));
        // final StructType groupsSchema = DataTypes.createStructType(groupsSchemaFields);

        final Dataset<Row> people = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "dd/MM/yy")
                .schema(peopleSchema)
                .csv(filePath + "data/people.csv");

        // final Dataset<Row> groups = spark
        //         .read()
        //         .option("header", "true")
        //         .option("delimiter", ",")
        //         .schema(groupsSchema)
        //         .csv(filePath + "data/groups.csv");

        people.show();
        // groups.show();
        

        // Q1. A 7 days moving average of participants’ age per nationality, 
        // computed for each day. For a given nationality, consider all the
        // groups in which at least one participant is of that nationality

        // Filter the dataset for individuals who joined within the last 7 days
        String today = java.time.LocalDate.now().toString();

        Dataset<Row> filteredPeople = people
                        .filter(expr("DateJoined >= date_sub('" + today + "', 6)"))
                        .filter(expr("DateJoined <= '" + today + "'"));

        // Calculate the average age per nationality
        Dataset<Row> todayMovingAverage = filteredPeople
                        .groupBy("Nationality")
                        .agg(round(avg("Age"), 2).as("todayMovingAverage"))
                        .withColumn("StartDate", date_sub(current_date(), 6))
                        .withColumn("EndDate", lit(java.time.LocalDate.now().toString()))
                        .orderBy("Nationality");

        if (useCache) {
                todayMovingAverage.cache();
        }

        todayMovingAverage.show();


        // Q2. Percentage increase (with respect to the day before) 
        // of the 7 days moving average, for each day

        String yesterday = java.time.LocalDate.now().minusDays(1).toString();

        // Now filter between yesterday and 6 days before yesterday
        filteredPeople = people
                .filter(expr("DateJoined >= date_sub('" + yesterday + "', 6)"))
                .filter(expr("DateJoined <= '" + yesterday + "'"));

        Dataset<Row> yesterdayMovingAverage = filteredPeople
                .groupBy("Nationality")
                .agg(round(avg("Age"), 2).as("YesterdayMovingAverage"))
                .orderBy("Nationality");

        // Join yesterday's and today's moving averages
        Dataset<Row> percentageIncrease = yesterdayMovingAverage
                .join(todayMovingAverage, "Nationality")
                .withColumn("PercentageIncrease",
                        round((col("TodayMovingAverage").divide(col("YesterdayMovingAverage")).minus(1)).multiply(100), 2))
                .withColumn("Date", lit(java.time.LocalDate.now().toString()))
                .orderBy("Nationality")
                .drop("StartDate", "EndDate");

        if (useCache) {
                percentageIncrease.cache();
        }

        percentageIncrease.show();
        
        // Q3. Top nationalities with the highest percentage increase 
        // of the seven days moving average, for each day

        // Define the window specification for ranking by descending percentage increase for each day
        WindowSpec windowSpec = Window.partitionBy("Date").orderBy(desc("PercentageIncrease"));

        // Rank the nationalities by percentage increase for each day
        Dataset<Row> rankedNationalities = percentageIncrease
                .withColumn("rank", rank().over(windowSpec))
                .filter(col("rank").leq(2)) // Keep only top 2 nationalities
                .drop("rank") // Drop the temporary rank column
                .select("Date", "Nationality", "PercentageIncrease") // Rearrange the columns
                .orderBy(col("Date"), desc("PercentageIncrease")); // Order by Date and PercentageIncrease

        if (useCache) {
                rankedNationalities.cache();
        }
        
        rankedNationalities.show();


        spark.close();
        
    }
}
