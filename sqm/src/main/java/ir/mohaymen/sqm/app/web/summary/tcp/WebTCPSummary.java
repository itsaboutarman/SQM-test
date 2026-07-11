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
public class WebTCPSummary extends Summary {
    public WebTCPSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Start_Time_s", "Country", "Region", "Province", "City", "BSC_Name", "RNC_Name", "ENODEB_ID",
                "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN", "Protocol", "AVG_DW_RTT_MICRO_SEC", "Application_Name",
                "AVG_UL_RTT_MICRO_SEC", "TCP_Connection_Status", "TCP_Connection_Release_Status", "APP_ID",
                "Protocol_Category_Name", "Uplink_Traffic", "Downlink_Traffic", "Total_TCP_Connect_RTT_ms",
                "Server_Side_Uplink_Lost_Packets", "Server_Side_Downlink_Lost_Packets", "User_Side_Uplink_Lost_Packets",
                "User_Side_Downlink_Lost_Packets", "TCP_Uplink_Packets_With_Payload",
                "TCP_Downlink_Packets_With_Payload", "Uplink_RTT_Calculations", "Downlink_RTT_Calculations",
                "Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms",
                "Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s", "Network_Uplink_Traffic",
                "Network_Downlink_Traffic", "TCP_Uplink_Retransmitted_Packets_with_Payload",
                "TCP_Downlink_Retransmitted_Packets_with_Payload", "Uplink_Packets",
                "Downlink_Packets", "SYN_ACK_Failures_During_TCP_Handshakes", "ACK_Failures_During_TCP_Handshakes",
                "User_Side_Hungry_Delay", "Server_Side_Hunger_Delay", "TCP_Downlink_Out_of_Order_Packets",
                "TCP_Uplink_Out_of_Order_Packets", "Delay_Between_First_TCP_and_First_GET_ms", "TCP_Connect_RTT_ms",
                "TCP_RTT_of_Step_1_ms", "Total_TCP_RTT_of_Step_1_ms", "Start_Time_ms", "End_Time_s", "End_Time_ms"
        );
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
                ds = ds.withColumn("Long_Down_RTTs", when(col("AVG_DW_RTT_MICRO_SEC").geq(1000).and(col("AVG_DW_RTT_MICRO_SEC").lt(1400)), 1)
                .when(col("AVG_DW_RTT_MICRO_SEC").geq(1400).and(col("AVG_DW_RTT_MICRO_SEC").lt(2000)), 2)
                .when(col("AVG_DW_RTT_MICRO_SEC").geq(2000).and(col("AVG_DW_RTT_MICRO_SEC").lt(2400)), 3)
                .when(col("AVG_DW_RTT_MICRO_SEC").geq(2400), 4));
        ds = ds.withColumn("Long_Down_RTTs", when(col("Long_Down_RTTs").isin(1, 2, 3, 4) ,col("Long_Down_RTTs")).otherwise(0));

        ds = ds.withColumn("Long_Up_RTTs", when(col("AVG_UL_RTT_MICRO_SEC").geq(500).and(col("AVG_UL_RTT_MICRO_SEC").lt(700)), 1)
                .when(col("AVG_UL_RTT_MICRO_SEC").geq(700).and(col("AVG_UL_RTT_MICRO_SEC").lt(1000)), 2)
                .when(col("AVG_UL_RTT_MICRO_SEC").geq(1000).and(col("AVG_UL_RTT_MICRO_SEC").lt(1400)), 3)
                .when(col("AVG_UL_RTT_MICRO_SEC").geq(1400), 4));
        ds = ds.withColumn("Long_Up_RTTs", when(col("Long_Up_RTTs").isin(1, 2, 3, 4) ,col("Long_Up_RTTs")).otherwise(0));

        ds = ds.withColumnRenamed("Protocol_Name", "Protocol");
        ds = ds.withColumnRenamed("Protocol_Category_Name", "Protocol_Category");
        ds = ds.withColumnRenamed("TCP_Connection_Status", "TCP_Connections_Status");
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Country", "Region", "Province", "City",
                "BSC_Name", "RNC_Name", "ENODEB_ID", "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN",
                "Protocol", "Long_Down_RTTs", "Long_Up_RTTs","TCP_Connections_Status",
                "TCP_Connection_Release_Status", "Application_Name", "Protocol_Category"});
        return ds;
    }

    Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes");
        ds = ds.groupBy("RECORD_DATE", "Country", "Region", "Province", "City",
                        "BSC_Name", "RNC_Name","ENODEB_ID",
                        "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN", "Protocol",
                        "Long_Down_RTTs", "Long_Up_RTTs", "TCP_Connections_Status", "TCP_Connection_Release_Status",
                        "Application_Name", "Protocol_Category")
                .agg(
                        sum("Uplink_Traffic").alias("Uplink_Traffic_Bytes"),
                        sum("Downlink_Traffic").alias("Downlink_Traffic_Bytes"),
                        sum("Total_TCP_Connect_RTT_ms").alias("Total_TCP_Connect_RTT_ms"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Server_Side_Uplink_Lost_Packets"))
                                .otherwise(0L)).alias("Server_Side_Uplink_TCP_Packet_Losses"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Server_Side_Downlink_Lost_Packets"))
                                .otherwise(0L)).alias("Server_Side_Downlink_TCP_Packet_Losses"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("User_Side_Uplink_Lost_Packets"))
                                .otherwise(0L)).alias("Client_Side_Uplink_TCP_Packet_Losses"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("User_Side_Downlink_Lost_Packets"))
                                .otherwise(0L)).alias("Client_Side_Downlink_TCP_Packet_Losses"),

                        sum("TCP_Uplink_Packets_With_Payload").alias("TCP_Uplink_Packets_With_Payload"),
                        sum("TCP_Downlink_Packets_With_Payload").alias("TCP_Downlink_Packets_With_Payload"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("AVG_UL_RTT_MICRO_SEC"))
                                .otherwise(0L)).alias("Uplink_Total_RTT"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("AVG_DW_RTT_MICRO_SEC"))
                                .otherwise(0L)).alias("Downlink_Total_RTT"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Uplink_RTT_Calculations"))
                                .otherwise(0L)).alias("Uplink_RTT_Times"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Downlink_RTT_Calculations"))
                                .otherwise(0L)).alias("Downlink_RTT_Times"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms"))
                                .otherwise(0L)).alias("Uplink_RTT_Long_Numbers"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s"))
                                .otherwise(0L)).alias("Downlink_RTT_Long_Numbers"),

                        sum("Network_Uplink_Traffic").alias("Network_Uplink_Traffic"),
                        sum("Network_Downlink_Traffic").alias("Network_Downlink_Traffic"),
                        sum("TCP_Uplink_Retransmitted_Packets_with_Payload").alias("TCP_Uplink_Retransmitted_Packets_with_Payload"),
                        sum("TCP_Downlink_Retransmitted_Packets_with_Payload").alias("TCP_Downlink_Retransmitted_Packets_with_Payload"),
                        sum("Uplink_Packets").alias("Uplink_Packets"),
                        sum("Downlink_Packets").alias("Downlink_Packets"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), 1).otherwise(0)).alias("TCP_Connection_Success_Count"),
                        sum(when(col("TCP_Connections_Status").isin(0, 1), 1).otherwise(0)).alias("Total_TCP_Connection_Count"),
                        sum(when(col("TCP_Connections_Status").equalTo(1), 1).otherwise(0)).alias("TCP_Connection_Failure_Count"),
                        sum(when(col("TCP_Connections_Status").equalTo(1), col("SYN_ACK_Failures_During_TCP_Handshakes")).otherwise(0))
                                .alias("Synack_Failure"),
                        sum(when(col("TCP_Connections_Status").equalTo(1),col("ACK_Failures_During_TCP_Handshakes")).otherwise(0))
                                .alias("Ack_Failure"),

                        sum("Long_Up_RTTs").alias("Long_Up_RTTs_Sum"),
                        sum("Long_Down_RTTs").alias("Long_Down_RTTs_Sum"),
                        sum("User_Side_Hungry_Delay").alias("User_Side_Hunger_Delay"),
                        sum("Server_Side_Hunger_Delay").alias("Server_Side_Hunger_Delay"),
                        sum("TCP_Downlink_Out_of_Order_Packets").alias("Downlink_Out_of_Order_Packets"),
                        sum("TCP_Uplink_Out_of_Order_Packets").alias("Uplink_Out_of_Order_Packets"),
                        sum("Delay_Between_First_TCP_and_First_GET_ms").alias("Delay_Between_First_TCP_and_First_GET_ms"),
                        sum("TCP_Connect_RTT_ms").alias("TCP_RTT"),
                        sum("Total_TCP_Connect_RTT_ms").alias("TOTAL_TCP_RTT"),
                        sum("TCP_RTT_of_Step_1_ms").alias("TCP_RTT_STEP1"),
                        sum("Total_TCP_RTT_of_Step_1_ms").alias("TOTAL_TCP_RTT_STEP1"),
                        sum("Start_Time_s").alias("Start_Time_s"),
                        sum("Start_Time_ms").alias("Start_Time_ms"),
                        sum("End_Time_s").alias("End_Time_s"),
                        sum("End_Time_ms").alias("End_Time_ms")

                );
        ds.printSchema();
        return ds;
    }
}
