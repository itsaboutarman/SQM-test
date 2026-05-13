package ir.mohaymen.sqm.app.voip.summary.overview;

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

@Job
public class VoIPOverviewSummary extends Summary {

    public VoIPOverviewSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "table_id", "Country", "Region", "Province", "City", "SGW", "PGW",
                "SGSN", "GGSN", "SGSN_POOL", "RAT", "TCP_Connection_Status", "Uplink_Traffic", "Downlink_Traffic",
                "Signaling_Consumption_Flag_Signaling_Messages", "TCP_Uplink_Packets_with_Payload",
                "TCP_Downlink_Packets_with_Payload", "User_Side_Uplink_Lost_Packets", "Server_Side_Uplink_Lost_Packets",
                "Server_Side_Downlink_Lost_Packets", "User_Side_Downlink_Lost_Packets", "Uplink_Valid_Traffic",
                "Uplink_Valid_Transmission_Duration", "Downlink_Valid_Traffic", "Downlink_Valid_Transmission_Duration");}

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("TCP_Connection_Status_Success" , when(col("TCP_Connection_Status").equalTo(0),1).otherwise(0))
                .withColumn("Signaling_Messages" , when(col("Signaling_Consumption_Flag_Signaling_Messages").equalTo(1), 1).otherwise(0))
                .withColumnRenamed("table_id","Table_ID");

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"RECORD_DATE", "Table_ID", "Country", "Region", "Province", "City", "SGW",
                "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "TCP_Connection_Status"});
        ds.printSchema();
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Table_ID", "Country", "Region", "Province", "City", "SGW", "PGW",
                        "SGSN", "GGSN", "SGSN_POOL", "RAT", "TCP_Connection_Status")
                .agg(
                        sum("Uplink_Traffic").alias("Uplink_Traffic"),
                        sum("Downlink_Traffic").alias("Downlink_Traffic"),
                        sum("TCP_Connection_Status_Success").alias("TCP_Connection_Success_Count"),
                        count(lit(1)).alias("All_TCP_Connections_Count"),
                        sum("Signaling_Messages").alias("Signaling_Messages_Count"),
                        sum("TCP_Uplink_Packets_with_Payload").alias("TCP_Uplink_Packets"),
                        sum("TCP_Downlink_Packets_with_Payload").alias("TCP_Downlink_Packets"),
                        sum("User_Side_Uplink_Lost_Packets").alias("User_Side_Uplink_Lost_Packets"),
                        sum("Server_Side_Uplink_Lost_Packets").alias("Server_Side_Uplink_Lost_Packets"),
                        sum("Server_Side_Downlink_Lost_Packets").alias("Server_Side_Downlink_Lost_Packets"),
                        sum("User_Side_Downlink_Lost_Packets").alias("User_Side_Downlink_Lost_Packets"),
                        sum("Uplink_Valid_Traffic").alias("Uplink_Valid_Traffic"),
                        sum("Uplink_Valid_Transmission_Duration").alias("Uplink_Valid_Transmission_Duration"),
                        sum("Downlink_Valid_Traffic").alias("Downlink_Valid_Traffic"),
                        sum("Downlink_Valid_Transmission_Duration").alias("Downlink_Valid_Transmission_Duration")
                );
        return ds;
    }
}
