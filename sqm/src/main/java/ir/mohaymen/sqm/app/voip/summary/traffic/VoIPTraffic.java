package ir.mohaymen.sqm.app.voip.summary.traffic;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

@Job
public class VoIPTraffic extends Summary{
    public VoIPTraffic(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    public Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "APN", "RAT", "APP_ID", "TCP_Connection_Status", "City", "Province",
                "Region", "Country", "Protocol_Name", "Protocol_Category_Name", "Brand", "Model", "Technology",
                "Type", "OS", "Uplink_Traffic", "Downlink_Traffic", "Uplink_Packets", "Downlink_Packets");
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.where(col("TCP_Connection_Status").isNotNull())
                .withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"15 minutes").getField("start"));

        ds = ds.withColumnRenamed("Type","Device_Type")
                .withColumnRenamed("TCP_Connection_Status","GET_Success_Flag")
                .withColumnRenamed("Protocol_Name","Protocol")
                .withColumnRenamed("Protocol_Category_Name","Protocol_Category");

        ds = aggregateFunc(ds);
        ds = calculateMetricFunc(ds);
        ds = generateKeyHash(ds, new String[]{
                "APN", "RAT", "APP_ID", "GET_Success_Flag", "City", "Province",
                "Region", "Country", "Protocol", "Protocol_Category", "Brand", "Model", "Technology",
                "Device_Type", "OS"});
        ds.printSchema();
        return ds;
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","APN", "RAT", "APP_ID", "GET_Success_Flag", "City", "Province",
                        "Region", "Country", "Protocol", "Protocol_Category", "Brand", "Model", "Technology",
                        "Device_Type", "OS")
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
        ds = ds.withColumn("uplink_traffic", col("uplink_traffic").divide(Math.pow(1024,4)))
                .withColumn("downlink_traffic", col("downlink_traffic").divide(Math.pow(1024,4)));
        return ds;
    }
}
