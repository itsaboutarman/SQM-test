package ir.mohaymen.sqm.app.ims.infregistersip.summary.analysistemp;

import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import ir.mohaymen.sqm.app.Job;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

@Job
public class IMSInfRegisterSIPAnalysisSummaryTemp extends Summary {
    public IMSInfRegisterSIPAnalysisSummaryTemp(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "A_SBC", "P_CSCF", "I_CSCF", "S_CSCF", "HSS", "AS", "MGCF", "MRFC", "eMSC", "ATCF", "MSC",
                "Access_Type", "Country", "RegionName", "ProvinceName", "CityName", "Reason_Code",
                "Release_Cause_Code", "Service_Status", "End_Time_ms");
    }



    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = ds.withColumn("SuccessCount", when(col("Service_Status").equalTo(0) , 1).otherwise(0))
                .withColumn("FailCount", when(col("Service_Status").equalTo(1) , 1).otherwise(0));

        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{
                "Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type",
                "Access_Type", "Country", "Region", "Province", "City", "Reason_Code",
                "Release_Cause_Code", "A_SBC", "P_CSCF", "I_CSCF", "S_CSCF", "HSS", "AS", "MGCF",
                "MRFC", "eMSC", "ATCF", "MSC"});

        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Table_id", "Registration_Type", "Service_Status",
                        "Interface_Type", "Source_NE_Type", "Destination_NE_Type",
                        "Access_Type", "Country", "Region", "Province", "City", "Reason_Code",
                        "Release_Cause_Code", "A_SBC", "P_CSCF", "I_CSCF", "S_CSCF", "HSS", "AS", "MGCF",
                        "MRFC", "eMSC", "ATCF", "MSC")
                .agg(
//                      Success Count
                        sum("SuccessCount").alias("SuccessCount"),
//                      Fail Count
                        sum("FailCount").alias("FailCount"),
//                      Total Count
                        sum(col("SuccessCount").plus(col("FailCount"))).alias("TotalCount"),
//                      Delay ms
                        sum(when(col("Service_Status").equalTo(0), col("End_Time_ms")).otherwise(0)).alias("Delay_ms")
                );
        return ds;
    }

}
