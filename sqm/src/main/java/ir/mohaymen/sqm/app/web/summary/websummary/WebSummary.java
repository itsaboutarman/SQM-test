package ir.mohaymen.sqm.app.web.summary.websummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

import static java.util.stream.Collectors.joining;
import java.util.Arrays;
import java.util.List;

@Job
public class WebSummary  extends Summary {

    public WebSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("First_GET_Success_or_Failure_Flag", ds.col("First_GET_Success_or_Failure_Flag").cast(DataTypes.StringType))
            .withColumn("First_GET_Transaction_Failure_Cause_Code", ds.col("First_GET_Transaction_Failure_Cause_Code").cast(DataTypes.StringType));

        ds = ds.na().fill(0, new String[] {"First_Page_Response_Delay_ms","PAGE_SUCCEED_FLAG",
                "Downlink_Valid_Data_Transmission_Duration","Page_Browsing_Delay",
                "TCP_Connect_RTT_ms","First_DNS_Response_Delay_Within_Page_ms",
                "Delay_Between_First_TCP_and_First_GET_ms"});

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("RECORD_DATE",window(col("RECORD_DATE"), "5 minutes")
                        .getField("start"));

        // TEMP!
        // ds = ds.filter(ds.col("RECORD_DATE").geq("2025-02-04 12:00:00"));
        // ds = ds.filter(ds.col("NON_EXISTENT").geq("2025-02-04 12:00:00"));
//---------------------------------------------------------------------------------------------------------------
//        Page Delay MSEL
        ds = ds.withColumn("PAGE_DELAY_MSEL"
                        , when((col("First_GET_Success_or_Failure_Flag").equalTo("0"))
                                        .and(col("Page_Size").geq(0))
                                , (col("Page_Browsing_Delay").multiply(1.0/1000000))
                                        .plus(col("TCP_Connect_RTT_ms"))
                                        .plus(col("First_DNS_Response_Delay_Within_Page_ms"))
                                        .plus(col("Delay_Between_First_TCP_and_First_GET_ms")))
                                .otherwise(0))
//        First Page Ack Number
                .withColumn("FST_PAGE_ACK_NUM"
                        , when((col("First_GET_Success_or_Failure_Flag").equalTo("0"))
                                .and(col("Page_Size").geq(0)), 1).otherwise(0));
//---------------------------------------------------------------------------------------------------------------
        ds = ds.na().fill(0, new String[] {"Page_Size"});

        ds = aggregateFunc(ds);

//        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{"RAT", "First_GET_Success_or_Failure_Flag", "failure_cause_code"
                , "City", "Province", "Region", "Country", "SGSN", "SGSN_POOL", "GGSN", "SGW", "PGW"});

        return ds;
    }


    @Override
    public Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Start_Time_s","RAT","First_GET_Success_or_Failure_Flag",
                "First_GET_Transaction_Failure_Cause_Code","First_Page_Response_Delay_ms",
                "Page_Size","Page_Browsing_Delay","TCP_Connect_RTT_ms","First_DNS_Response_Delay_Within_Page_ms",
                "Delay_Between_First_TCP_and_First_GET_ms","Downlink_Valid_Data_Transmission_Duration",
                "PAGE_SUCCEED_FLAG","ERROR_CODE_4xx_TIMES","ERROR_CODE_5xx_TIMES","GET_TIMEOUT_NUM",
                "POST_TO_NUM","City","Province","Region","Country","SGSN","SGSN_POOL","GGSN","SGW","PGW"
        );
    }

    public Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE",
                        "RAT","First_GET_Success_or_Failure_Flag",
                        "First_GET_Transaction_Failure_Cause_Code","City","Province","Region","Country",
                        "SGSN","SGSN_POOL","GGSN","SGW","PGW")
                .agg(
                        count(when(col("First_GET_Success_or_Failure_Flag").equalTo("0")
                                , col("First_GET_Success_or_Failure_Flag"))).alias("successful_requests"),
                        count(when(col("First_GET_Success_or_Failure_Flag").isin("0","1")
                                , col("First_GET_Success_or_Failure_Flag"))).alias("total_request"),
                        sum("First_Page_Response_Delay_ms").alias("first_page_response_delay_ms"),
                        count(when(col("PAGE_SUCCEED_FLAG").equalTo(0)
                                , col("PAGE_SUCCEED_FLAG"))).alias("page_browsing_successes"),
                        sum("PAGE_DELAY_MSEL").alias("PAGE_DELAY_MSEL"),
                        sum("FST_PAGE_ACK_NUM").alias("FST_PAGE_ACK_NUM"),
                        sum(when((col("Downlink_Valid_Data_Transmission_Duration").gt(0))
                                .and(col("First_GET_Success_or_Failure_Flag").equalTo(0))
                                .and(col("Page_Size").divide(1024).geq(50))
                                , col("Page_Size")).otherwise(0)).alias("T_Page_Size"),
                        sum(when((col("First_GET_Success_or_Failure_Flag").equalTo(0))
                                        .and(col("Page_Size").divide(1024).geq(50))
                                , col("Downlink_Valid_Data_Transmission_Duration"))
                                .otherwise(0)).alias("T_Downlink_Valid_Data_Transmission_Duration"),
                        sum(when(col("PAGE_SUCCEED_FLAG").equalTo(1)
                                , col("ERROR_CODE_4xx_TIMES")).otherwise(0)).alias("4xx_error"),
                        sum(when(col("PAGE_SUCCEED_FLAG").equalTo(1)
                                , col("ERROR_CODE_5xx_TIMES")).otherwise(0)).alias("5xx_error"),
                        sum(when(col("PAGE_SUCCEED_FLAG").equalTo(1)
                                , col("GET_TIMEOUT_NUM").plus(col("POST_TO_NUM")))
                                .otherwise(0)).alias("1000_timeout"),
                        count(lit(1)).alias("total_count")
                );
        ds = ds.withColumnRenamed("First_GET_Transaction_Failure_Cause_Code", "failure_cause_code")
                .withColumnRenamed("T_Page_Size", "page_size")
                .withColumnRenamed("T_Downlink_Valid_Data_Transmission_Duration", "Downlink_Valid_Data_Transmission_Duration");
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
        return ds;
    }
}