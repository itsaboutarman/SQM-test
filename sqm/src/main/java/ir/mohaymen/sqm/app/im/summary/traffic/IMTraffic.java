package ir.mohaymen.sqm.app.im.summary.traffic;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

@Job
public class IMTraffic extends Summary {
    public IMTraffic(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","APN", "RAT","TCP_Connection_Status","APP_ID","Country","Region","Province",
                "City","Protocol_Category_Name","Protocol_Name","Brand","Model","Technology","Type","OS",
                "Uplink_Traffic","Downlink_Traffic","Uplink_Packets","Downlink_Packets");}

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.where(col("TCP_Connection_Status").isNotNull())
                .withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"15 minutes").getField("start"))
                .withColumnRenamed("Protocol_Category_Name" ,"Protocol_Category")
                .withColumnRenamed("Protocol_Name" ,"Protocol")
                .withColumnRenamed("Type" ,"Device_Type");
        ds = aggregateFunc(ds);
        ds = calculateMetricFunc(ds);
        ds = generateKeyHash(ds, new String[]{"APN", "RAT","TCP_Connection_Status","APP_ID","Country","Region","Province",
                "City","Protocol_Category","Protocol","Brand","Model","Technology","Device_Type","OS"});
        return ds;
    }
    private Dataset<Row> aggregateFunc(Dataset<Row> ds){
        return ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","APN", "RAT","TCP_Connection_Status","APP_ID","Country","Region","Province",
                        "City","Protocol_Category","Protocol","Brand","Model","Technology","Device_Type","OS")
                .agg(
                       sum("Uplink_Traffic").alias("uplink_traffic"),
                       sum("Downlink_Traffic").alias("downlink_traffic"),
                        sum("Uplink_Packets").alias("uplink_packets"),
                        sum("Downlink_Packets").alias("downlink_packets"),
                        count(lit(1)).alias("total_count")
                );
    }
    private  Dataset<Row> calculateMetricFunc(Dataset<Row> ds){
        return ds.withColumn("uplink_traffic" ,col("uplink_traffic").divide(Math.pow(1024,4)))
                .withColumn("downlink_traffic", col("downlink_traffic").divide(Math.pow(1024, 4)));
    }
}

