package ir.mohaymen.sqm.app.other.summary.overview;

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
public class OtherOverviewSummary extends Summary {
    public OtherOverviewSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select( "RECORD_DATE","Country", "Region", "Province", "City", "SGW", "PGW", "SGSN",
                "GGSN", "SGSN_POOL", "RAT", "L4_UL_THROUGHPUT", "L4_DW_THROUGHPUT", "SERVER_PROBE_UL_LOST_PKT" ,
                "SERVER_PROBE_DW_LOST_PKT", "USER_PROBE_UL_LOST_PKT", "USER_PROBE_DW_LOST_PKT",
                "TCP_UL_PACKAGES_WITHPL", "TCP_DW_PACKAGES_WITHPL", "AVG_UL_RTT", "AVG_DW_RTT", "UL_RTT_STAT_NUM",
                "DW_RTT_STAT_NUM", "TCP_CONN_STATES", "L7_UL_GOODPUT_FULL_MSS", "DATATRANS_UL_DURATION",
                "L7_DW_GOODPUT_FULL_MSS", "DATATRANS_DW_DURATION");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.where(col("TCP_CONN_STATES").isNotNull());
        ds = ds.withColumn("TCP_Connection_Status_Success" ,
                        when(col("TCP_CONN_STATES").equalTo(0),1).otherwise(0));

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds , new String[]{ "Country", "Region", "Province", "City", "SGW", "PGW",
                "SGSN", "GGSN", "SGSN_POOL", "RAT"});
        ds.printSchema();
        return ds;
    }

    Dataset<Row> aggregateFunc(Dataset<Row> ds){
        ds = ds.withWatermark("RECORD_DATE" , "1440 minutes");
        ds = ds.groupBy("RECORD_DATE" , "Country", "Region", "Province", "City", "SGW", "PGW", "SGSN",
                        "GGSN", "SGSN_POOL", "RAT")
                .agg(
                        sum("L4_UL_THROUGHPUT").alias("Uplink_Traffic_Bytes"),
                        sum("L4_DW_THROUGHPUT").alias("Downlink_Traffic_Bytes"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("SERVER_PROBE_UL_LOST_PKT"))
                                .otherwise(0L)).alias("Server_Side_Uplink_TCP_Packet_Losses"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("SERVER_PROBE_DW_LOST_PKT"))
                                .otherwise(0L)).alias("Server_Side_Downlink_TCP_Packet_Losses"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("USER_PROBE_UL_LOST_PKT"))
                                .otherwise(0L)).alias("Client_Side_Uplink_TCP_Packet_Losses"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("USER_PROBE_DW_LOST_PKT"))
                                .otherwise(0L)).alias("Client_Side_Downlink_TCP_Packet_Losses"),
                        sum("TCP_UL_PACKAGES_WITHPL").alias("TCP_Uplink_Packets"),
                        sum("TCP_DW_PACKAGES_WITHPL").alias("TCP_Downlink_Packets"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("AVG_UL_RTT").multiply(col("UL_RTT_STAT_NUM")))
                                .otherwise(0L)).alias("Uplink_Total_RTT"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("AVG_DW_RTT").multiply(col("DW_RTT_STAT_NUM")))
                                .otherwise(0L)).alias("Downlink_Total_RTT"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("UL_RTT_STAT_NUM"))
                                .otherwise(0L)).alias("Uplink_RTT_Times"),
                        sum(when(col("TCP_CONN_STATES").equalTo(0), col("DW_RTT_STAT_NUM"))
                                .otherwise(0L)).alias("Downlink_RTT_Times"),
                        sum("TCP_Connection_Status_Success").alias("TCP_Connection_Success_Count"),
                        count(lit(1)).alias("Total_TCP_Connections_Count"),

                        sum("L7_UL_GOODPUT_FULL_MSS").alias("Uplink_Valid_Volume"),
                        sum("DATATRANS_UL_DURATION").alias("Uplink_Valid_Transmission_Duration"),
                        sum("L7_DW_GOODPUT_FULL_MSS").alias("Downlink_Valid_Volume"),
                        sum("DATATRANS_DW_DURATION").alias("Downlink_Valid_Transmission_Duration")
                );
        return ds;
    }
}
