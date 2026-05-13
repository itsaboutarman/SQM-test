package ir.mohaymen.sqm.app.sns.summary.snsoverviewsummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.sum;

@Job
public class SNSOverviewSummary extends Summary {
    public SNSOverviewSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select( "RECORD_DATE","Country","Region","Province","City", "SGW","PGW","SGSN",
                "GGSN","SGSN_POOL","RAT", "L4_UL_THROUGHPUT","L4_DW_THROUGHPUT" ,"SIGNALING_USE_FLAG" ,"SERVER_PROBE_UL_LOST_PKT" ,"SERVER_PROBE_DW_LOST_PKT"
                ,"USER_PROBE_UL_LOST_PKT" ,"USER_PROBE_DW_LOST_PKT" ,"TCP_UL_PACKAGES_WITHPL" ,"TCP_DW_PACKAGES_WITHPL" ,
                "AVG_UL_RTT" ,"AVG_DW_RTT" ,"UL_RTT_STAT_NUM","DW_RTT_STAT_NUM");
    }
    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds , new String[]{
                "Country","Region","Province","City","SGW","PGW","SGSN", "GGSN","SGSN_POOL","RAT"
        });
        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }
    Dataset<Row> aggregateFunc(Dataset<Row> ds){
        ds = ds.withWatermark("RECORD_DATE" , "1440 minutes");
        ds = ds.groupBy("RECORD_DATE" , "Country","Region","Province","City","SGW","PGW","SGSN",
                        "GGSN","SGSN_POOL","RAT")
                .agg(
                        sum("L4_UL_THROUGHPUT").alias("Uplink_Traffic_Bytes"),
                        sum("L4_DW_THROUGHPUT").alias("Downlink_Traffic_Bytes"),
                        sum("SIGNALING_USE_FLAG").alias("Total_Signaling_Times"),
                        sum("SERVER_PROBE_UL_LOST_PKT").alias("Server_Side_Uplink_TCP_Packet_Losses"),
                        sum("SERVER_PROBE_DW_LOST_PKT").alias("Server_Side_Downlink_TCP_Packet_Losses"),
                        sum("USER_PROBE_UL_LOST_PKT").alias("Client_Side_Uplink_TCP_Packet_Losses"),
                        sum("USER_PROBE_DW_LOST_PKT").alias("Client_Side_Downlink_TCP_Packet_Losses"),
                        sum("TCP_UL_PACKAGES_WITHPL").alias("TCP_Uplink_Packets"),
                        sum("TCP_DW_PACKAGES_WITHPL").alias("TCP_Downlink_Packets"),
                        sum(col("AVG_UL_RTT").multiply(col("UL_RTT_STAT_NUM"))).alias("Uplink_Total_RTT"),
                        sum(col("AVG_DW_RTT").multiply(col("DW_RTT_STAT_NUM"))).alias("Downlink_Total_RTT"),
                        sum("UL_RTT_STAT_NUM").alias("Uplink_RTT_Times"),
                        sum("DW_RTT_STAT_NUM").alias("Downlink_RTT_Times")
                        );
        return ds;
    }
}
