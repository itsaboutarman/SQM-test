package ir.mohaymen.sqm.app.streaming.summary.traffic;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.joining;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Job
public class Traffic extends Summary {

    public Traffic(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.filter(col("GET_STREAMING_FLAG").isNotNull());

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("RECORD_DATE",window(col("RECORD_DATE"), "15 minutes")
                        .getField("start"));

        ds = ds.withColumnRenamed("Protocol_name","Protocol").withColumnRenamed("Protocol_Category_name","Protocol_Category")
                .withColumnRenamed("TER_BRAND_NAME","Brand").withColumnRenamed("TER_MODEL_NAME","Model")
                .withColumnRenamed("TER_MODENAME","Technology").withColumnRenamed("TER_TYPE_NAME_EN","Device_Type")
                .withColumnRenamed("TER_OS_TYPE_NAME","OS");

        ds = ds.na().fill(0, new String[] {"Uplink_Traffic","Downlink_Traffic","Uplink_Packets","Downlink_Packets"});

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{
                "APN","RAT","APP_ID","GET_STREAMING_FLAG",
                "City","Province","Region","Country","Protocol",
                "Protocol_Category","Brand","Model",
                "Technology","Device_Type","OS"});

        return ds;
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","APN","RAT","APP_ID","GET_STREAMING_FLAG",
                        "City","Province","Region","Country","Protocol",
                        "Protocol_Category","Brand","Model",
                        "Technology","Device_Type","OS")
                .agg(
                        sum("Uplink_Traffic").alias("uplink_traffic"),
                        sum("Downlink_Traffic").alias("downlink_traffic"),
                        sum("Uplink_Packets").alias("uplink_packets"),
                        sum("Downlink_Packets").alias("downlink_packets"),
                        count(lit(1)).alias("total_count")
                );
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
//                      Uplink Traffic (TB)
        ds = ds.withColumn("uplink_traffic", col("uplink_traffic").divide(Math.pow(1024,4)))
//                      Downlink Traffic (TB)
                .withColumn("downlink_traffic", col("downlink_traffic").divide(Math.pow(1024,4)));
        return ds;
    }

    @Override
    public Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Start_Time_s","APN","Uplink_Traffic","Downlink_Traffic","Uplink_Packets",
                "Downlink_Packets","RAT","GET_STREAMING_FLAG",
                "APP_ID","City","Province","Region","Country","Protocol_name","Protocol_Category_name","TER_BRAND_NAME",
                "TER_MODEL_NAME","TER_MODENAME","TER_TYPE_NAME_EN","TER_OS_TYPE_NAME"
        );
    }
}
