package ir.mohaymen.sqm.app.im.summary.imsummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class IMSummary extends Summary {
    public IMSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select( "RECORD_DATE","Country","Region","Province","City", "SGW","PGW","SGSN",
                "GGSN","SGSN_POOL","RAT","TCP_CONNECTION_STATUS",
                "SIGNALING_CONSUMPTION_FLAG_SIGNALING_MESSAGES" ,"UPLINK_TRAFFIC","DOWNLINK_TRAFFIC",
                "TCP_UPLINK_PACKETS_WITH_PAYLOAD","TCP_DOWNLINK_PACKETS_WITH_PAYLOAD","USER_SIDE_UPLINK_LOST_PACKETS",
                "SERVER_SIDE_UPLINK_LOST_PACKETS_","SERVER_SIDE_DOWNLINK_LOST_PACKETS","USER_SIDE_DOWNLINK_LOST_PACKETS","UPLINK_VALID_TRAFFIC",
                "UPLINK_VALID_TRANSMISSION_DURATION","DOWNLINK_VALID_TRAFFIC","DOWNLINK_VALID_TRANSMISSION_DURATION");
    }
    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.filter(col("TCP_CONNECTION_STATUS").isNotNull())
                .withColumn("TCP_CONNECTION_Success" , when(col("TCP_CONNECTION_STATUS").equalTo(0),1).otherwise(0))
                .withColumn("Signaling_Messages" , when(col("SIGNALING_CONSUMPTION_FLAG_SIGNALING_MESSAGES").equalTo(0),1).otherwise(0))
                .withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds , new String[]{
                "Country","Region","Province","City","SGW","PGW","SGSN", "GGSN","SGSN_POOL","RAT"
        });
        return ds;
    }
    Dataset<Row> aggregateFunc(Dataset<Row> ds){
        ds = ds.withWatermark("RECORD_DATE" , "1440 minutes");
        ds = ds.groupBy("RECORD_DATE" , "Country","Region","Province","City","SGW","PGW","SGSN",
                        "GGSN","SGSN_POOL","RAT")
                .agg(
                        sum("UPLINK_TRAFFIC").alias("Uplink_Traffic"),
                        sum("DOWNLINK_TRAFFIC").alias("Downlink_Traffic"),
                        sum("TCP_CONNECTION_Success").alias("TCP_Connection_Success_Count"),
                        count(lit(1)).alias("All_TCP_Connections_Count"),
                        sum("Signaling_Messages").alias("Signaling_Messages_Count"),
                        sum("TCP_UPLINK_PACKETS_WITH_PAYLOAD").alias("TCP_Uplink_Packets"),
                        sum("TCP_DOWNLINK_PACKETS_WITH_PAYLOAD").alias("TCP_Downlink_Packets"),
                        sum("USER_SIDE_UPLINK_LOST_PACKETS").alias("User_Side_Uplink_Lost_Packets"),
                        sum("SERVER_SIDE_UPLINK_LOST_PACKETS_").alias("Server_Side_Uplink_Lost_Packets"),
                        sum("SERVER_SIDE_DOWNLINK_LOST_PACKETS").alias("Server_Side_Downlink_Lost_Packets"),
                        sum("USER_SIDE_DOWNLINK_LOST_PACKETS").alias("User_Side_Downlink_Lost_Packets"),
                        sum("UPLINK_VALID_TRAFFIC").alias("Uplink_Valid_Traffic"),
                        sum("UPLINK_VALID_TRANSMISSION_DURATION").alias("Uplink_Valid_Transmission_Duration"),
                        sum("DOWNLINK_VALID_TRAFFIC").alias("Downlink_Valid_Traffic"),
                        sum("DOWNLINK_VALID_TRANSMISSION_DURATION").alias("Downlink_Valid_Transmission_Duration")
                );
        return ds;
    }
}
