package ir.mohaymen.sqm.app.dns.summary.dnsoverviewsummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class DNSOverviewSummary extends Summary {

    public DNSOverviewSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE"
                , "Country", "Region", "Province", "City", "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL",
                "Brand", "Model", "Technology", "Type", "OS", "Protocol", "Protocol_Category", "RAT","DNS_Server_Name"
                , "FST_TRANS_FLAG", "ERROR_CODE_FST", "DNS_TRANS_NUM", "DNS_SUCCEED_NUM", "DNS_OK_DELAY_TIME"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        // TEMP!
        ds = ds.filter(ds.col("RECORD_DATE").geq("2025-04-19 00:30:00.0"));


        ds = ds.withColumnRenamed("FST_TRANS_FLAG", "First_Transaction_Success_or_Failure_Flag")
                .withColumnRenamed("ERROR_CODE_FST","First_Request_Failure_Cause_Value");
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Country", "Region", "Province", "City"
                , "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL"
                , "Brand", "Model", "Technology", "Type", "OS"
                , "Protocol", "Protocol_Category", "RAT","DNS_Server_Name"
                , "First_Transaction_Success_or_Failure_Flag", "First_Request_Failure_Cause_Value"});
        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE"
                        , "Country", "Region", "Province", "City"
                        , "SGW", "PGW", "SGSN", "GGSN", "SGSN_POOL"
                        , "Brand", "Model", "Technology", "Type", "OS"
                        , "Protocol", "Protocol_Category", "RAT","DNS_Server_Name"
                        , "First_Transaction_Success_or_Failure_Flag", "First_Request_Failure_Cause_Value")
                .agg(
                        sum("DNS_TRANS_NUM").alias("DNS_Requests"),
                        sum("DNS_SUCCEED_NUM").alias("DNS_Request_Success_Times"),
                        sum(when(col("First_Transaction_Success_or_Failure_Flag").equalTo(0),
                                        col("DNS_OK_DELAY_TIME")).otherwise(0)).alias("Total_Delay_of_Successful_DNS_Requests_ns")
                );
        return ds;
    }
}