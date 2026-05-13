package ir.mohaymen.sqm.app.s1mme.summary.emm;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import static org.apache.spark.sql.functions.*;

import java.util.Map;

@Job
public class S1MME_EMM_Transaction extends Summary {

    public S1MME_EMM_Transaction(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "MME_Name", "RoamingType", "DeviceBrand", "DeviceModel",
                "DeviceType", "DeviceSupportedMode", "DeviceOS", "RegionName", "ProvinceName", "CityName", "RAI_TAI",
                "SiteName", "CellName", "APN", "FIR_FAIL_TRANS", "FIR_FAIL_CAUSE_TYPE", "FIR_FAIL_CAUSE",
                "EMM_Transaction", "EMM_SUB_Transaction",
                "EMM_Status", "EMM_REQ_TIME_SEC", "EMM_REQ_TIME_MSEC", "EMM_END_TIME_SEC", "EMM_END_TIME_MSEC"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.filter(
            (col("RECORD_DATE").geq(lit("2024-08-21 04:00:00")))
            .and(col("RECORD_DATE").lt(lit("2024-08-21 06:00:00"))));

        ds = ds.filter(col("EMM_Status").isin("Successful","Rejected"));

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes")
                .getField("start"));

//---------------------------------------------------------------------------------------------------------------
//        Success Count
        ds = ds.withColumn("SuccessCount", when(col("EMM_Status").equalTo("Successful")
                        , 1).otherwise(0))
//        Fail Count
                .withColumn("FailCount", when(col("EMM_Status").equalTo("Rejected")
                        , 1).otherwise(0))
//        Delay(ms)
                .withColumn("Delay_ms", when(col("EMM_Status").equalTo("Successful")
                        , (col("EMM_END_TIME_SEC").multiply(1000).plus(col("EMM_END_TIME_MSEC")))
                                .minus(col("EMM_REQ_TIME_SEC").multiply(1000).plus(col("EMM_REQ_TIME_MSEC"))))
                        .otherwise(0));
//---------------------------------------------------------------------------------------------------------------

        ds = aggregateFunc(ds);

        ds = calculateMetricFunc(ds);

        ds = generateKeyHash(ds, new String[]{
            "MME_Name", "RoamingType", "DeviceBrand", "DeviceModel",
            "DeviceType", "DeviceSupportedMode", "DeviceOS", "RegionName", "ProvinceName", "CityName", "RAI_TAI",
            "SiteName", "CellName", "APN", "FIR_FAIL_TRANS", "FIR_FAIL_CAUSE_TYPE", "FIR_FAIL_CAUSE",
            "EMM_Transaction", "EMM_SUB_Transaction"
        });

        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }



    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "MME_Name", "RoamingType", "DeviceBrand", "DeviceModel",
                        "DeviceType", "DeviceSupportedMode", "DeviceOS", "RegionName", "ProvinceName", "CityName", "RAI_TAI",
                        "SiteName", "CellName", "APN", "FIR_FAIL_TRANS", "FIR_FAIL_CAUSE_TYPE", "FIR_FAIL_CAUSE",
                        "EMM_Transaction", "EMM_SUB_Transaction")
                .agg(
//                      Success Count
                        sum("SuccessCount").alias("SuccessCount"),
//                      Fail Count
                        sum("FailCount").alias("FailCount"),
//                      Count
                        count(lit(1)).alias("TotalCount"),
//                      Delay(ms)
                        sum("Delay_ms").alias("Delay_ms")
                );
        return ds;
    }

    private Dataset<Row> calculateMetricFunc(Dataset<Row> ds) {
        return ds;
    }
}
