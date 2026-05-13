package ir.mohaymen.sqm.app.tdrmap.summary.checkimei;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;

import java.util.Map;

@Job
public class TDRCheckIMEI extends Summary {

    public TDRCheckIMEI(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE"
                , "SRVSTAT", "OPC", "OGT", "DPC", "DGT", "SRVTYPE"
                , "SRVRSPTIME", "REL_TYPE", "CAUSE", "SIG_COLLECTION_TYPE", "PD"
                , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS"
                , "Home_SGSN_Pool_Name", "SGSN_Name"
                , "trans_cause_det"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.filter(col("SRVTYPE").equalTo(11))
                .withColumnRenamed("trans_cause_det","TRANS_CAUSE_DETAILS");
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));

//---------------------------------------------------------------------------------------------------------------
//        SRVSTAT Non-Null Times
        ds = ds.withColumn("SRVSTAT_Times_NoNull", when(col("SRVSTAT").isin(0, 1)
                        , 1).otherwise(0))
//        Request Successful Times
                .withColumn("Request_Successful_Times", when(col("SRVSTAT").equalTo(0)
                        , 1).otherwise(0))
//        Black Response Times
                .withColumn("Black_Response_Times",
                        when(col("CAUSE").equalTo(1).and(col("SRVSTAT").equalTo(0))
                                , 1).otherwise(0))
//        White Response Times
                .withColumn("White_Response_Times",
                        when(col("CAUSE").equalTo(0).and(col("SRVSTAT").equalTo(0))
                                , 1).otherwise(0))
//        Timeout Response Times
                .withColumn("Timeout_Response_Times",
                        when(col("CAUSE").equalTo(1000).and(col("SRVSTAT").equalTo(1))
                                , 1).otherwise(0))
//        Response Time
                .withColumn("Response_Time", when(col("SRVSTAT").equalTo(0)
                        , col("SRVRSPTIME")).otherwise(0));
//---------------------------------------------------------------------------------------------------------------

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{"SRVSTAT", "OPC", "OGT", "DPC", "DGT", "SRVTYPE"
                , "REL_TYPE", "TRANS_CAUSE_DETAILS"
                , "SIG_COLLECTION_TYPE", "PD"
                , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS"
                , "Home_SGSN_Pool_Name", "SGSN_Name"});
        
        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "SRVSTAT", "OPC", "OGT", "DPC", "DGT", "SRVTYPE"
                        , "REL_TYPE", "TRANS_CAUSE_DETAILS"
                        , "SIG_COLLECTION_TYPE", "PD"
                        , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS"
                        , "Home_SGSN_Pool_Name", "SGSN_Name")
                .agg(
//                      Trans Succeed Flag Non-Null Times
                        sum("SRVSTAT_Times_NoNull").alias("SRVSTAT_Times_NoNull"),
//                      Total Success Request
                        sum("Request_Successful_Times").alias("Total_Success_Request"),
//                      Total Black Response
                        sum("Black_Response_Times").alias("Total_Black_Response"),
//                      Total White Response
                        sum("White_Response_Times").alias("Total_White_Response"),
//                      Total Timeout Response
                        sum("Timeout_Response_Times").alias("Total_Timeout_Response"),
//                      Total Response Time
                        sum("Response_Time").alias("Total_Response_Time"),
//                      Count
                        count(lit(1)).alias("Total_Request")
                );
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
        return ds;
    }
}
