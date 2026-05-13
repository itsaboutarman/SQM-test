package ir.mohaymen.sqm.app.web.summary.tcp;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;

@Job
public class WebTCP extends Summary {

    public WebTCP(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> applicationRef = references.get("applicationRef");
        applicationRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));

        ds = ds.na().fill(0, new String[] {"Uplink_Traffic","Downlink_Traffic"});

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = joinWithReference(ds, applicationRef);

        ds = generateKeyHash(ds, new String[]{
                "RAT","APN","Protocol_Category_name","APP_ID",
                "City","Province","Region", "Country",
                "SGSN","SGSN_POOL","GGSN","SGW","PGW"});

        return ds;
    }

    private Dataset<Row> joinWithReference(Dataset<Row> ds, Dataset<Row> reference) {
        Column cond = (ds.col("APP_ID").equalTo(col("tcp_ref.Application_ID")));
        ds = ds.join(broadcast(reference.as("tcp_ref")),cond,"leftouter");
        return ds;
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        int windowTimeMinute = 15;
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy(
                        window(col("RECORD_DATE"), String.format("%d minutes",windowTimeMinute))
                                .getField("start").alias("RECORD_DATE"),
                        col("RAT"),col("APN"),col("Protocol_Category_name"),col("APP_ID"),
                        col("City"),col("Province"),col("Region"),col("Country"),
                        col("SGSN"),col("SGSN_POOL"),col("GGSN"),col("SGW"),col("PGW")
                )
                .agg(
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                        .and(col("Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms").gt(0)),
                                col("Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms"))).alias("Uplink_RTT_Long_Times_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Uplink_RTT_Calculations"))).alias("Uplink_RTT_Count_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                        .and(col("Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s").gt(0)),
                                col("Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s"))).alias("Downlink_RTT_Long_Times_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Downlink_RTT_Calculations"))).alias("Downlink_RTT_Count_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Average_Uplink_RTT"))).alias("Uplink_Total_RTT_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Average_Downlink_RTT"))).alias("Downlink_Total_RTT_Http"),
                        sum(col("TCP_Connect_RTT_ms")).alias("TCP_Connection_Delay_Http"),
                        sum(col("Downlink_Traffic")).alias("Downlink_Effective_Volume_Http"),
                        sum(col("Uplink_Traffic")).alias("Uplink_Effective_Volume_Http"),
                        sum(col("Network_Downlink_Traffic")).alias("Downlink_Network_Volume_Http"),
                        sum(col("Network_Uplink_Traffic")).alias("Uplink_Network_Volume_Http"),
                        sum(col("TCP_Downlink_Packets_with_Payload")).alias("TCP_Downlink_Packets_with_Payload_Http"),
                        sum(col("TCP_Uplink_Packets_with_Payload")).alias("TCP_Uplink_Packets_with_Payload_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("User_Side_Downlink_Lost_Packets"))).alias("Client_Side_Downlink_TCP_Packet_Losses_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("User_Side_Uplink_Lost_Packets"))).alias("Client_Side_Uplink_TCP_Packet_Losses_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Server_Side_Uplink_Lost_Packets"))).alias("Server_Side_Uplink_TCP_Packet_Losses_Http"),
                        sum(when((col("TCP_Connection_Status").equalTo(0))
                                , col("Server_Side_Downlink_Lost_Packets"))).alias("Server_Side_Downlink_TCP_Packet_Losses_Http"),
                        sum(when(col("TCP_Connection_Status").equalTo(1), 1)).alias("TCP_Connection_Failure_Http"),
                        sum(when(col("TCP_Connection_Status").isin(0, 1), 1)).alias("TCP_CONN_REQ_HTTP"),
                        sum(when(col("TCP_Connection_Status").equalTo(0), 1)).alias("TCP_CONN_SUCC_HTTP"),
                        (sum(col("TCP_Downlink_Retransmitted_Packets_with_Payload"))
                                .divide(sum(col("TCP_Downlink_Packets_with_Payload")))).alias("Downlink_TCP_Retransmission_Rate"),
                        sum(col("TCP_Downlink_Retransmitted_Packets_with_Payload")).alias("TCP_Downlink_Retransmitted_Packets_with_Payload_Http"),
                        sum(col("TCP_Uplink_Retransmitted_Packets_with_Payload")).alias("TCP_Uplink_Retransmitted_Packets_with_Payload_Http"),
                        (sum(col("Downlink_Traffic")).divide(Math.pow(1024,3))).alias("Downlink_Effective_Volume_GBytes"),
                        (sum(col("Uplink_Traffic")).divide(Math.pow(1024,3))).alias("Uplink_Effective_Volume_GBytes"),
                        sum(col("Uplink_Packets")).alias("Uplink_Effective_Packets_Http"),
                        sum(col("Downlink_Packets")).alias("Downlink_Effective_Packets_Http"),
                        (sum(col("Network_Uplink_Traffic")).divide(Math.pow(1024,3))).alias("Uplink_Network_Volume_GBytes"),
                        (sum(col("Network_Downlink_Traffic")).divide(Math.pow(1024,3))).alias("Downlink_Network_Volume_GBytes")
                );
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
        return ds;
    }

    @Override
    public Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Start_Time_s","Country","Region","Province","City","SGSN","SGSN_Pool","GGSN","SGW","PGW","RAT","Protocol_Category_name",
                "APP_ID","APN","Uplink_Traffic","Downlink_Traffic","Network_Uplink_Traffic","Network_Downlink_Traffic","Uplink_Packets","Downlink_Packets",
                "TCP_Connection_Status","TCP_Connect_RTT_ms","TCP_Uplink_Retransmitted_Packets_with_Payload","TCP_Downlink_Retransmitted_Packets_with_Payload",
                "TCP_Uplink_Packets_with_Payload","TCP_Downlink_Packets_with_Payload","Average_Uplink_RTT","Average_Downlink_RTT",
                "Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms","Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s",
                "Uplink_RTT_Calculations","Downlink_RTT_Calculations","User_Side_Uplink_Lost_Packets","Server_Side_Uplink_Lost_Packets",
                "Server_Side_Downlink_Lost_Packets","User_Side_Downlink_Lost_Packets"
        );
    }
}
