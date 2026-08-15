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
        return ds.select("RECORD_DATE", "Country", "Region", "Province", "City", "BSC_Name", "RNC_Name", "ENODEB_ID",
                "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN", "Protocol_Name", "Application_Name",
                "Uplink_Traffic", "Downlink_Traffic", "Signaling_Consumption_Flag_Signaling_Messages",
                "Server_Side_Uplink_Lost_Packets", "Server_Side_Downlink_Lost_Packets", "User_Side_Uplink_Lost_Packets",
                "User_Side_Downlink_Lost_Packets", "Uplink_Valid_Traffic", "Uplink_Valid_Transmission_Duration",
                "Downlink_Valid_Traffic", "Downlink_Valid_Transmission_Duration", "Uplink_Packets", "Downlink_Packets");
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"15 minutes").getField("start"));

        ds = ds.withColumnRenamed("ENODEB_ID", "ENodeB")
                .withColumnRenamed("RNC_Name", "RNC")
                .withColumnRenamed("BSC_Name", "BSC")
                .withColumnRenamed("Protocol_Name", "Protocol");

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{
                "Country", "Region", "Province", "City", "BSC", "RNC", "ENodeB",
                "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN", "Protocol", "Application_Name"});
        ds.printSchema();
        return ds;
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Country", "Region", "Province", "City", "BSC", "RNC", "ENodeB",
                        "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL", "RAT", "APN", "Protocol", "Application_Name")
                .agg(
                        sum("Uplink_Traffic").alias("Uplink_Traffic_Bytes"),
                        sum("Downlink_Traffic").alias("Downlink_Traffic_Bytes"),
                        sum("Signaling_Consumption_Flag_Signaling_Messages").alias("Signaling_message_counts"),
                        sum("Server_Side_Uplink_Lost_Packets").alias("Server_Side_Uplink_TCP_Packet_Losses"),
                        sum("Server_Side_Downlink_Lost_Packets").alias("Server_Side_Downlink_TCP_Packet_Losses"),
                        sum("User_Side_Uplink_Lost_Packets").alias("Client_Side_Uplink_TCP_Packet_Losses"),
                        sum("User_Side_Downlink_Lost_Packets").alias("Client_Side_Downlink_TCP_Packet_Losses"),
                        sum("Uplink_Valid_Traffic").alias("Uplink_Effective_Traffic"),
                        sum("Uplink_Valid_Transmission_Duration").alias("Uplink_Valid_Transmission_Duration"),
                        sum("Downlink_Valid_Traffic").alias("Downlink_Effective_Traffic"),
                        sum("Downlink_Valid_Transmission_Duration").alias("Downlink_Valid_Transmission_Duration"),
                        sum("Uplink_Packets").alias("Uplink_Packets"),
                        sum("Downlink_Packets").alias("Downlink_Packets")
                );
        return ds;
    }
}
