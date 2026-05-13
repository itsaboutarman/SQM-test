package ir.mohaymen.sqm.app.dns.summary.traffic;

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
public class DNSTraffic extends Summary {
    public DNSTraffic(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","APN", "RAT","FST_TRANS_FLAG","Country","Region","Province",
                "City","Protocol_Category","Protocol","Brand","Model","Technology","Type","OS",
                "L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_PACKETS","L4_DW_PACKETS");}
    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.where(col("FST_TRANS_FLAG").isNotNull())
                .withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"15 minutes").getField("start"))
                .withColumnRenamed("Type" ,"Device_Type")
                .withColumnRenamed("L4_UL_THROUGHPUT","uplink_traffic")
                .withColumnRenamed("L4_DW_THROUGHPUT","downlink_traffic")
                .withColumnRenamed("L4_UL_PACKETS","uplink_packets")
                .withColumnRenamed("L4_DW_PACKETS","downlink_packets")
                .withColumn("APP_ID" ,lit(null));
        ds = aggregateFunc(ds);
        ds = calculateMetricFunc(ds);
        ds = generateKeyHash(ds, new String[]{"APN", "RAT","FST_TRANS_FLAG",
                "APP_ID","Country","Region","Province", "City","Protocol_Category","Protocol",
                "Brand","Model","Technology", "Device_Type","OS"});
        return ds;
    }
    private Dataset<Row> aggregateFunc(Dataset<Row> ds){
        return ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","APN", "RAT","FST_TRANS_FLAG",
                        "APP_ID","Country","Region","Province", "City","Protocol_Category","Protocol","Brand","Model","Technology","Device_Type","OS")
                .agg(
                        sum("uplink_traffic").alias("uplink_traffic"),
                        sum("downlink_traffic").alias("downlink_traffic"),
                        sum("uplink_packets").alias("uplink_packets"),
                        sum("downlink_packets").alias("downlink_packets"),
                        count(lit(1)).alias("total_count")
                );
    }
    private  Dataset<Row> calculateMetricFunc(Dataset<Row> ds){
        return ds.withColumn("uplink_traffic" ,col("uplink_traffic").divide(Math.pow(1024,4)))
                .withColumn("downlink_traffic", col("downlink_traffic").divide(Math.pow(1024, 4)));
    }
}
