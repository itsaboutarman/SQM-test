package ir.mohaymen.sqm.app.streaming.summary.tcp;

import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ir.mohaymen.sqm.app.Job;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

@Job
public class StreamingTCPSummary extends Summary {
    public StreamingTCPSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Country", "Region", "Province", "City",
                "BSC_Name", "RNC_Name","ENODEB_ID","Brand",
                "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL",
                "RAT", "APN_Edit", "Protocol_Name", "AVG_DW_RTT", "AVG_UL_RTT", "TCP_CONN_STATES", "TCP_STATES",
                "Application_Name", "Protocol_Category_Name", "L4_UL_THROUGHPUT", "L4_DW_THROUGHPUT",
                "TOTAL_TCP_RTT", "SERVER_PROBE_UL_LOST_PKT", "SERVER_PROBE_DW_LOST_PKT",
                "USER_PROBE_UL_LOST_PKT", "USER_PROBE_DW_LOST_PKT", "TCP_UL_PACKAGES_WITHPL", "TCP_DW_PACKAGES_WITHPL",
                "AVG_UL_RTT", "AVG_DW_RTT", "UL_RTT_STAT_NUM", "DW_RTT_STAT_NUM",
                "UL_RTT_LONG_NUM", "DW_RTT_LONG_NUM", "NETWORK_UL_TRAFFIC", "NETWORK_DL_TRAFFIC",
                "TCP_UL_RETRANS_WITHPL", "TCP_DW_RETRANS_WITHPL", "L4_UL_PACKETS", "L4_DW_PACKETS",
                "TCP_CONN_2_FAILED_TIMES", "TCP_CONN_3_FAILED_TIMES",
                "USER_HUNGRY_DELAY", "SERVER_HUNGRY_DELAY", "TCP_DW_OUTOFSEQU", "TCP_UL_OUTOFSEQU",
                "MS_ACK_TO_1STGET_DELAY", "TCP_RTT", "TOTAL_TCP_RTT", "TCP_RTT_STEP1", "TOTAL_TCP_RTT_STEP1",
                "BEGIN_TIME", "BEGIN_TIME_MSEL", "END_TIME", "END_TIME_MSEL");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumnRenamed("TCP_STATES", "TCP_Connection_Release_Status");
        ds = ds.withColumnRenamed("APN_Edit", "APN");

        ds = ds.withColumn("Long_Down_RTTs", when(col("AVG_DW_RTT").geq(1000).and(col("AVG_DW_RTT").lt(1400)), 1)
                .when(col("AVG_DW_RTT").geq(1400).and(col("AVG_DW_RTT").lt(2000)), 2)
                .when(col("AVG_DW_RTT").geq(2000).and(col("AVG_DW_RTT").lt(2400)), 3)
                .when(col("AVG_DW_RTT").geq(2400), 4));
        ds = ds.withColumn("Long_Down_RTTs", when(col("Long_Down_RTTs").isin(1, 2, 3, 4), col("Long_Down_RTTs")).otherwise(0));

        ds = ds.withColumn("Long_Up_RTTs",when(col("AVG_UL_RTT").geq(500).and(col("AVG_UL_RTT").lt(700)), 1)
                .when(col("AVG_UL_RTT").geq(700).and(col("AVG_UL_RTT").lt(1000)), 2)
                .when(col("AVG_UL_RTT").geq(1000).and(col("AVG_UL_RTT").lt(1400)), 3)
                .when(col("AVG_UL_RTT").geq(1400), 4));
        ds = ds.withColumn("Long_Up_RTTs", when(col("Long_Up_RTTs").isin(1, 2, 3, 4), col("Long_Up_RTTs")).otherwise(0));

        ds = ds.withColumnRenamed("Protocol_Name", "Protocol")
                .withColumnRenamed("Protocol_Category_Name", "Protocol_Category")
                .withColumnRenamed("TCP_CONN_STATES", "TCP_Connections_Status");
        ds = ds.withColumn("Brand_Category" ,when(col("Brand").isin("SAMSUNG","XIAOMI" , "APPLE", "Unknown"),
                when(col("Brand").isin("SAMSUNG","XIAOMI" , "APPLE"),lit("Common")).otherwise(lit("Not specified")))
                .otherwise(lit("Rare")));

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"))
                .withColumn("RAN",
                when(col("RAT").equalTo(2), "GERAN")
                        .when(col("RAT").equalTo(1), "UTRAN")
                        .when(col("RAT").equalTo(6), "EUTRAN")
                        .when(col("RAT").isNull().or(col("RAT").equalTo(0)), "not carried")
                        .otherwise(null)
        );

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Country", "Region", "Province", "City",
                "BSC_Name", "RNC_Name", "ENODEB_ID", "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN",
                "Protocol", "Long_Down_RTTs", "Long_Up_RTTs", "TCP_Connections_Status",
                "TCP_Connection_Release_Status", "Application_Name", "Protocol_Category","Brand_Category","RAN"});
        return ds;
    }

    Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes");
        ds = ds.groupBy("RECORD_DATE", "Country", "Region", "Province", "City",
                        "BSC_Name", "RNC_Name", "ENODEB_ID", "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN",
                        "Protocol", "Long_Down_RTTs", "Long_Up_RTTs", "TCP_Connections_Status",
                        "TCP_Connection_Release_Status", "Application_Name", "Protocol_Category","Brand_Category","RAN")
                .agg(
                        sum("L4_UL_THROUGHPUT").alias("Uplink_Traffic_Bytes"),
                        sum("L4_DW_THROUGHPUT").alias("Downlink_Traffic_Bytes"),
                        sum("TOTAL_TCP_RTT").alias("Total_TCP_Connect_RTT_ms"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("SERVER_PROBE_UL_LOST_PKT"))
                                .otherwise(0L)).alias("Server_Side_Uplink_TCP_Packet_Losses"),
                        sum(when(col("TCP_Connections_Status").equalTo(0), col("SERVER_PROBE_DW_LOST_PKT"))
                                .otherwise(0L)).alias("Server_Side_Downlink_TCP_Packet_Losses"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("USER_PROBE_UL_LOST_PKT"))
                                .otherwise(0L)).alias("Client_Side_Uplink_TCP_Packet_Losses"),
                        sum(when(col("TCP_Connections_Status").equalTo(0), col("USER_PROBE_DW_LOST_PKT"))
                                .otherwise(0L)).alias("Client_Side_Downlink_TCP_Packet_Losses"),

                        sum("TCP_UL_PACKAGES_WITHPL").alias("TCP_Uplink_Packets_With_Payload"),
                        sum("TCP_DW_PACKAGES_WITHPL").alias("TCP_Downlink_Packets_With_Payload"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("AVG_UL_RTT"))
                                .otherwise(0L)).alias("Uplink_Total_RTT"),
                        sum(when(col("TCP_Connections_Status").equalTo(0), col("AVG_DW_RTT"))
                                .otherwise(0L)).alias("Downlink_Total_RTT"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("UL_RTT_STAT_NUM"))
                                .otherwise(0L)).alias("Uplink_RTT_Times"),
                        sum(when(col("TCP_Connections_Status").equalTo(0), col("DW_RTT_STAT_NUM"))
                                .otherwise(0L)).alias("Downlink_RTT_Times"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), col("UL_RTT_LONG_NUM"))
                                .otherwise(0L)).alias("Uplink_RTT_Long_Numbers"),
                        sum(when(col("TCP_Connections_Status").equalTo(0), col("DW_RTT_LONG_NUM"))
                                .otherwise(0L)).alias("Downlink_RTT_Long_Numbers"),

                        sum("NETWORK_UL_TRAFFIC").alias("Network_Uplink_Traffic"),
                        sum("NETWORK_DL_TRAFFIC").alias("Network_Downlink_Traffic"),
                        sum("TCP_UL_RETRANS_WITHPL").alias("TCP_Uplink_Retransmitted_Packets_with_Payload"),
                        sum("TCP_DW_RETRANS_WITHPL").alias("TCP_Downlink_Retransmitted_Packets_with_Payload"),
                        sum("L4_UL_PACKETS").alias("Uplink_Packets"),
                        sum("L4_DW_PACKETS").alias("Downlink_Packets"),

                        sum(when(col("TCP_Connections_Status").equalTo(0), 1).otherwise(0)).alias("TCP_Connection_Success_Count"),
                        sum(when(col("TCP_Connections_Status").isin(0, 1), 1).otherwise(0)).alias("Total_TCP_Connection_Count"),
                        sum(when(col("TCP_Connections_Status").equalTo(1), 1).otherwise(0)).alias("TCP_Connection_Failure_Count"),

                        sum(when(col("TCP_Connections_Status").equalTo(1), col("TCP_CONN_2_FAILED_TIMES")).otherwise(0))
                                .alias("Synack_Failure"),
                        sum(when(col("TCP_Connections_Status").equalTo(1), col("TCP_CONN_3_FAILED_TIMES")).otherwise(0))
                                .alias("Ack_Failure"),

                        sum("Long_Up_RTTs").alias("Long_Up_RTTs_Sum"),
                        sum("Long_Down_RTTs").alias("Long_Down_RTTs_Sum"),
                        sum("USER_HUNGRY_DELAY").alias("User_Side_Hunger_Delay"),
                        sum("SERVER_HUNGRY_DELAY").alias("Server_Side_Hunger_Delay"),
                        sum("TCP_DW_OUTOFSEQU").alias("Downlink_Out_of_Order_Packets"),
                        sum("TCP_UL_OUTOFSEQU").alias("Uplink_Out_of_Order_Packets"),
                        sum("MS_ACK_TO_1STGET_DELAY").alias("Delay_Between_First_TCP_and_First_GET_ms"),
                        sum("TCP_RTT").alias("TCP_RTT"),
                        sum("TOTAL_TCP_RTT").alias("TOTAL_TCP_RTT"),
                        sum("TCP_RTT_STEP1").alias("TCP_RTT_STEP1"),
                        sum("TOTAL_TCP_RTT_STEP1").alias("TOTAL_TCP_RTT_STEP1"),
                        sum("BEGIN_TIME").alias("Start_Time_s"),
                        sum("BEGIN_TIME_MSEL").alias("Start_Time_ms"),
                        sum("END_TIME").alias("End_Time_s"),
                        sum("END_TIME_MSEL").alias("End_Time_ms")

                );
        ds.printSchema();
        return ds;
    }
}