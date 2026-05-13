package ir.mohaymen.sqm.app.s6a.summary.checkimei;

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
public class S6ACheckIMEI extends Summary {

    public S6ACheckIMEI(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE"
                , "TRANS_TYPE", "TRANS_REQ_TIME_SEC", "TRANS_REQ_TIME_MSEC", "TRANS_RSP_TIME_SEC"
                , "TRANS_RSP_TIME_MSEC", "TRANS_SUCCED_FLAG"
                , "TRANS_CAUSE"
                , "trans_cause_det"
                , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS", "MME_Name"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.filter(col("TRANS_TYPE").equalTo(35))
                .withColumnRenamed("trans_cause_det","TRANS_CAUSE_DETAILS");
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));

//---------------------------------------------------------------------------------------------------------------
//        Transaction Succeed Flag Non-Null Times
        ds = ds.withColumn("TRANS_SUCCED_FLAG_Times_NoNull", when(col("TRANS_SUCCED_FLAG").isin(0, 1)
                        , 1).otherwise(0))
//        Request Successful Times
                .withColumn("Request_Successful_Times", when(col("TRANS_SUCCED_FLAG").equalTo(0)
                        , 1).otherwise(0))
//        Black Response Times
                .withColumn("Black_Response_Times", when(col("TRANS_CAUSE").equalTo(6001)
                        , 1).otherwise(0))
//        White Response Times
                .withColumn("White_Response_Times", when(col("TRANS_CAUSE").equalTo(6000)
                        , 1).otherwise(0))
//        Timeout Response Times
                .withColumn("Timeout_Response_Times", when(col("TRANS_CAUSE").equalTo(9000)
                        , 1).otherwise(0))
//        Response Time
                .withColumn("Response_Time"
                        , (col("TRANS_RSP_TIME_SEC").multiply(lit(1000)).plus(col("TRANS_RSP_TIME_MSEC")))
                                .minus(col("TRANS_REQ_TIME_SEC").multiply(lit(1000)).plus(col("TRANS_REQ_TIME_MSEC"))));
//---------------------------------------------------------------------------------------------------------------

        ds = aggregateFunc(ds);

//        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{"TRANS_TYPE", "TRANS_SUCCED_FLAG", "TRANS_CAUSE_DETAILS"
                , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS", "MME_Name"});

        System.out.println("output schema:");
        ds.printSchema();
        
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                // RECORD_DATE is TRANS_REQ_TIME_SEC dim in docs
                .groupBy("RECORD_DATE", "TRANS_TYPE", "TRANS_SUCCED_FLAG", "TRANS_CAUSE_DETAILS"
                        , "DeviceBrand", "DeviceModel", "DeviceTechnology", "DeviceType", "DeviceOS", "MME_Name")
                .agg(
//                      Trans Succeed Flag Non-Null Times
                        sum("TRANS_SUCCED_FLAG_Times_NoNull").alias("TRANS_SUCCED_FLAG_Times_NoNull"),
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
}