package ir.mohaymen.sqm.app.streaming.summary.video;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.exporters.BatchIdListener;
import ir.mohaymen.sqm.utils.SparkSessionHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;
import java.util.concurrent.atomic.AtomicLong;

import java.util.Map;
import java.util.Arrays;

@Job
public class Video extends Summary {

    public Video(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Start_Time_s", "RAT", "Downlink_Packets", "TCP_Connect_RTT_ms", "TCP_Downlink_Retransmitted_Packets",
                "GET_STREAMING_FLAG", "GET_STREAMING_DELAY", "INITIAL_BUFFER_SUCCESS_FLAG", "INITIAL_BUFFER_DELAY",
                "VIDEO_STREAMING_STALL_FREQUENCY", "STREAMING_MEDIA_STALL_DURATION", "VALID_TRAFFIC_DURING_VIDEO_STREAMING",
                "VALID_DATA_TRANSMISSION_DURATION", "STREAMING_MEDIA_PLAYING_DURATION", "PLAYING_STATUS_INDICATOR", "FAILURE_CODE",
                "TCP_DOWNLINK_RETRANSMITTED_PACKETS_WITH_PAYLOAD", "TCP_DOWNLINK_PACKETS_WITH_PAYLOAD", "DNS_QUERY_DELAY_MILLISECONDS",
                "STREAMING_DISCONNECT_FLAG", "SGSN", "SGSN_POOL", "GGSN", "SGW", "PGW", "Country", "Region", "Province", "City"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        

//---------------------------------------------------------------------------------------------------------------
//        GET Successful Request Response SR
        ds = ds.withColumn("GET_Request_Successful_Times", when(col("GET_STREAMING_FLAG").equalTo("0")
                        , 1).otherwise(0))
//        GET Request Response SR
                .withColumn("GET_Request_Times", when(col("GET_STREAMING_FLAG").isin("0", "1")
                        , 1).otherwise(0).cast("double"))
//        GET Successful Initial Buffer SR
                .withColumn("Initial_Buffer_Successful_Times", when(col("INITIAL_BUFFER_SUCCESS_FLAG").equalTo("0")
                        , 1).otherwise(0))
//        GET Initial Buffer SR
                .withColumn("Initial_Buffer_Times", when(col("INITIAL_BUFFER_SUCCESS_FLAG").isin("0", "1")
                        , 1).otherwise(0).cast("double"))
//        Playing Status Indicator
                .withColumn("PLAYING_STATUS_INDICATOR_Times", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , 1).otherwise(0).cast("double"))
//        Video Streaming Stall Freq (Only when PLAYING_STATUS_INDICATOR isn't null)
                .withColumn("VIDEO_STREAMING_STALL_FREQUENCY", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , col("VIDEO_STREAMING_STALL_FREQUENCY")).otherwise(0))
//        Streaming Media Playing Duration (Only when PLAYING_STATUS_INDICATOR isn't null)
                .withColumn("STREAMING_MEDIA_PLAYING_DURATION", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , col("STREAMING_MEDIA_PLAYING_DURATION")).otherwise(0))
//        Streaming Media Stall Duration (Only when PLAYING_STATUS_INDICATOR isn't null)
                .withColumn("STREAMING_MEDIA_STALL_DURATION", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , col("STREAMING_MEDIA_STALL_DURATION")).otherwise(0))
//         Valid Traffic During Video Streaming (Only when PLAYING_STATUS_INDICATOR isn't null)
                .withColumn("VALID_TRAFFIC_DURING_VIDEO_STREAMING", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , col("VALID_TRAFFIC_DURING_VIDEO_STREAMING")).otherwise(0))
//         Valid Data Transmission Duration (Only when PLAYING_STATUS_INDICATOR isn't null)
                .withColumn("VALID_DATA_TRANSMISSION_DURATION", when(col("PLAYING_STATUS_INDICATOR").isNotNull()
                        , col("VALID_DATA_TRANSMISSION_DURATION")).otherwise(0))
//        DISCONNECT FLAG Times
                .withColumn("DISCONNECT_FLAG_Times", when(col("STREAMING_DISCONNECT_FLAG").equalTo("0")
                                .and(col("PLAYING_STATUS_INDICATOR").isNotNull()),
                        1).otherwise("0"))
//      DNS Query Delay for Successful flags
                .withColumn("DNS_QUERY_DELAY_MILLISECONDS", when(col("INITIAL_BUFFER_SUCCESS_FLAG").equalTo("0")
                        , col("DNS_QUERY_DELAY_MILLISECONDS")).otherwise(0))

//      GEY STREAMING Delay for Successful flags
                .withColumn("GET_STREAMING_DELAY", when(col("INITIAL_BUFFER_SUCCESS_FLAG").equalTo("0")
                        , col("GET_STREAMING_DELAY")).otherwise(0))
//      Initial Buffer Delay for Successful flags
                .withColumn("INITIAL_BUFFER_DELAY", when(col("INITIAL_BUFFER_SUCCESS_FLAG").equalTo("0")
                        , col("INITIAL_BUFFER_DELAY")).otherwise(0));
//---------------------------------------------------------------------------------------------------------------

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{"RECORD_DATE", "SGSN", "SGSN_POOL", "GGSN", "SGW",
                        "PGW", "Country", "Region", "Province", "City", "RAT", "FAILURE_CODE",
                        "GET_STREAMING_FLAG", "INITIAL_BUFFER_SUCCESS_FLAG", "STREAMING_DISCONNECT_FLAG"});

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "SGSN", "SGSN_POOL", "GGSN", "SGW",
                        "PGW", "Country", "Region", "Province", "City", "RAT", "FAILURE_CODE",
                        "GET_STREAMING_FLAG", "INITIAL_BUFFER_SUCCESS_FLAG", "STREAMING_DISCONNECT_FLAG")
                .agg(
//                      GET Request Successful Times
                        sum("GET_Request_Successful_Times").alias("GET_Request_Successful_Times"),
//                        GET Request Times
                        sum("GET_Request_Times").alias("GET_Request_Times"),
//                      GET Initial Buffer Successful Times
                        sum("Initial_Buffer_Successful_Times").alias("Initial_Buffer_Successful_Times"),
//                      GET Initial Buffer Times
                        sum("Initial_Buffer_Times").alias("Initial_Buffer_Times"),
//                      Count
                        count(lit(1)).alias("Total_Count"),
//                      DNS Query Delay
                        sum("DNS_QUERY_DELAY_MILLISECONDS").alias("TOTAL_DNS_QUERY_DELAY_MILLISECONDS"),
//                      GET Response Delay
                        sum("GET_STREAMING_DELAY").alias("TOTAL_GET_STREAMING_DELAY"),
//                      Initial Buffer Delay
                        sum("INITIAL_BUFFER_DELAY").alias("TOTAL_INITIAL_BUFFER_DELAY"),
//                      Stall Count
                        sum("VIDEO_STREAMING_STALL_FREQUENCY").alias("TOTAL_VIDEO_STREAMING_STALL_FREQUENCY"),
//                      Streaming Play Duration
                        sum("STREAMING_MEDIA_PLAYING_DURATION").alias("TOTAL_STREAMING_MEDIA_PLAYING_DURATION"),
//                      Stall Duration
                        sum("STREAMING_MEDIA_STALL_DURATION").alias("TOTAL_STREAMING_MEDIA_STALL_DURATION"),
//                      Video Streaming Disconnections
                        sum("DISCONNECT_FLAG_Times").alias("Video_Streaming_Disconnections"),
//                      Video Streaming Plays
                        sum("PLAYING_STATUS_INDICATOR").alias("Video_Streaming_Plays"),
//                      VALID TRAFFIC DURING VIDEO STREAMING
                        sum("VALID_TRAFFIC_DURING_VIDEO_STREAMING").alias("TOTAL_VALID_TRAFFIC_DURING_VIDEO_STREAMING"),
//                      VALID DATA TRANSMISSION DURATION
                        sum("VALID_DATA_TRANSMISSION_DURATION").alias("TOTAL_VALID_DATA_TRANSMISSION_DURATION"),
//                      Playing Status Indicator
                        sum("PLAYING_STATUS_INDICATOR_Times").alias("TOTAL_PLAYING_STATUS_INDICATOR_Times"),


                        sum("Downlink_Packets").alias("TOTAL_Downlink_Packets"),
                        sum("TCP_Downlink_Retransmitted_Packets").alias("TOTAL_TCP_Downlink_Retransmitted_Packets"),
                        sum("TCP_DOWNLINK_RETRANSMITTED_PACKETS_WITH_PAYLOAD").alias("TOTAL_TCP_DOWNLINK_RETRANSMITTED_PACKETS_WITH_PAYLOAD"),
                        sum("TCP_DOWNLINK_PACKETS_WITH_PAYLOAD").alias("TOTAL_TCP_DOWNLINK_PACKETS_WITH_PAYLOAD")
                );
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
//                      Video Streaming Start Delay
        ds = ds.withColumn("DNS_Query_Delay", col("TOTAL_DNS_QUERY_DELAY_MILLISECONDS")
                        .divide(col("Initial_Buffer_Successful_Times")))
                .withColumn("GET_Response_Delay", col("TOTAL_GET_STREAMING_DELAY")
                        .divide(col("Initial_Buffer_Successful_Times")))
                .withColumn("Initial_Buffer_Delay", col("TOTAL_INITIAL_BUFFER_DELAY")
                        .divide(col("Initial_Buffer_Successful_Times")))
                .withColumn("Video_Streaming_Start_Delay", col("DNS_Query_Delay")
                        .plus(col("GET_Response_Delay")).plus(col("Initial_Buffer_Delay")))
//                      Video Streaming Stall Frequency
                .withColumn("Video_Streaming_Stall_Frequency", col("TOTAL_VIDEO_STREAMING_STALL_FREQUENCY")
                        .multiply(60 * 1000).divide(col("TOTAL_STREAMING_MEDIA_PLAYING_DURATION")))
//                      Video Streaming Stall Time Rate
                .withColumn("Video_Streaming_Stall_Time_Rate", col("TOTAL_STREAMING_MEDIA_STALL_DURATION")
                        .divide(col("TOTAL_STREAMING_MEDIA_PLAYING_DURATION")))
//                      Video Streaming Plays Disconnection Rate
                .withColumn("Video_Streaming_Plays_Disconnection_Rate", col("Video_Streaming_Disconnections")
                        .divide(col("Video_Streaming_Plays")))
//                      Video Streaming Download Throughput
                .withColumn("Video_Streaming_Download_Throughput", col("TOTAL_VALID_TRAFFIC_DURING_VIDEO_STREAMING")
                        .multiply(8 * 1024).divide(col("TOTAL_VALID_DATA_TRANSMISSION_DURATION")).divide(1000));

        ds = ds.withColumn("GET_Request_Times_NoNull", col("GET_Request_Times"))
                .withColumn("Initial_Buffer_Times_NoNull", col("Initial_Buffer_Times"))
                .drop("GET_Request_Times", "Initial_Buffer_Times");
        ds = ds.withColumn("TOTAL_DNS_QUERY_DELAY_MILLISECONDS"
                , when(col("TOTAL_DNS_QUERY_DELAY_MILLISECONDS").lt(0),  new java.math.BigDecimal("999999999999999999"))
                        .otherwise(col("TOTAL_DNS_QUERY_DELAY_MILLISECONDS")))
                .withColumn("TOTAL_VALID_DATA_TRANSMISSION_DURATION"
                , when(col("TOTAL_VALID_DATA_TRANSMISSION_DURATION").lt(0),  new java.math.BigDecimal("999999999999999999"))
                        .otherwise(col("TOTAL_VALID_DATA_TRANSMISSION_DURATION")))
                .withColumn("TOTAL_VALID_TRAFFIC_DURING_VIDEO_STREAMING"
                , when(col("TOTAL_VALID_TRAFFIC_DURING_VIDEO_STREAMING").lt(0),  new java.math.BigDecimal("999999999999999999"))
                        .otherwise(col("TOTAL_VALID_TRAFFIC_DURING_VIDEO_STREAMING")))
                .withColumn("Video_Streaming_Download_Throughput"
                , when(col("Video_Streaming_Download_Throughput").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Download_Throughput")))
                .withColumn("Video_Streaming_Plays_Disconnection_Rate"
                , when(col("Video_Streaming_Plays_Disconnection_Rate").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Plays_Disconnection_Rate")))
                .withColumn("Video_Streaming_Stall_Time_Rate"
                , when(col("Video_Streaming_Stall_Time_Rate").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Stall_Time_Rate")))
                .withColumn("Video_Streaming_Stall_Frequency"
                , when(col("Video_Streaming_Stall_Frequency").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Stall_Frequency")))
                .withColumn("Video_Streaming_Start_Delay"
                , when(col("Video_Streaming_Start_Delay").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Start_Delay")))
                .withColumn("Initial_Buffer_Delay"
                , when(col("Initial_Buffer_Delay").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Initial_Buffer_Delay")))
                .withColumn("GET_Response_Delay"
                , when(col("GET_Response_Delay").lt(0), Double.MAX_VALUE)
                        .otherwise(col("GET_Response_Delay")))
                .withColumn("DNS_Query_Delay"
                , when(col("DNS_Query_Delay").lt(0), Double.MAX_VALUE)
                        .otherwise(col("DNS_Query_Delay")))
                .withColumn("Video_Streaming_Plays"
                , when(col("Video_Streaming_Plays").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Plays")))
                .withColumn("Video_Streaming_Disconnections"
                , when(col("Video_Streaming_Disconnections").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Video_Streaming_Disconnections")))
                .withColumn("GET_Request_Times_NoNull"
                , when(col("GET_Request_Times_NoNull").lt(0), Double.MAX_VALUE)
                        .otherwise(col("GET_Request_Times_NoNull")))
                .withColumn("Initial_Buffer_Times_NoNull"
                , when(col("Initial_Buffer_Times_NoNull").lt(0), Double.MAX_VALUE)
                        .otherwise(col("Initial_Buffer_Times_NoNull")))
                .withColumn("TOTAL_PLAYING_STATUS_INDICATOR_Times"
                , when(col("TOTAL_PLAYING_STATUS_INDICATOR_Times").lt(0), Double.MAX_VALUE)
                        .otherwise(col("TOTAL_PLAYING_STATUS_INDICATOR_Times")));
        return ds;
    }
}
