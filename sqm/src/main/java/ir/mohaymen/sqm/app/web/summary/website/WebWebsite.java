package ir.mohaymen.sqm.app.web.summary.website;

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
public class WebWebsite extends Summary {

    public WebWebsite(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("RECORD_DATE",window(col("RECORD_DATE"), "15 minutes")
                        .getField("start"))
                .withColumnRenamed("Protocol_Category_name", "Protocol_Category");
        // TEMP!
        ds = ds.filter(ds.col("RECORD_DATE").geq("2025-02-04 12:00:00"));

        ds = ds.na().fill(0, new String[] {"Uplink_Traffic","Downlink_Traffic"});

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{"Protocol_Category","APP_ID","Host_Name"});

        return ds;
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Protocol_Category","APP_ID","Host_Name")
                .agg(
                        sum("Uplink_Traffic").alias("uplink_traffic"),
                        sum("Downlink_Traffic").alias("downlink_traffic"),
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
        return ds.select("Start_Time_s","Uplink_Traffic","Downlink_Traffic", "Protocol_Category_name"
                , "Host_Name","APP_ID"
        );
    }
}
