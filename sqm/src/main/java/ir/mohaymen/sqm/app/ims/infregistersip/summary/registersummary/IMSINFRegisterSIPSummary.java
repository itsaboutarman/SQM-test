package ir.mohaymen.sqm.app.ims.infregistersip.summary.registersummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class IMSINFRegisterSIPSummary extends Summary {

    public IMSINFRegisterSIPSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Start_Time_s","Start_Time_ms", "IMEI","Session_id",
                "Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type",
                "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country",
                "RegionName", "ProvinceName", "CityName", "TAI", "SITE", "CELL", "Authentication_Mode",
                "Release_Cause_Code", "Abnormal_Release_Information", "Abnormal_Release_Cause",
                "First_Release_NE_Type",
                "End_Time_ms", "Registration_Request_Valid_Duration_s", "Registration_Response_Valid_Duration_s"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));

        // ds =ds.filter(col("RECORD_DATE").gt("2025-04-30 18:35:00").and(col("Start_Time_s").lt(1767273194)));

        ds = ds.withColumn("SuccessCount", when(col("Service_Status").equalTo(0)
                        , 1).otherwise(0))
                .withColumn("FailCount", when(col("Service_Status").equalTo(1)
                        , 1).otherwise(0));

        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);

        ds = generateKeyHash(ds, new String[]{
                "Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type",
                "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country",
                "Region", "Province", "City", "TAI", "SITE", "CELL", "Authentication_Mode",
                "Release_Cause_Code", "Abnormal_Release_Information", "Abnormal_Release_Cause",
                "First_Release_NE_Type"});

        System.out.println("output schema:");
        ds.printSchema();
        
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE",
                        "Table_id", "Registration_Type", "Service_Status",
                        "Interface_Type", "Source_NE_Type", "Destination_NE_Type",
                        "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country",
                        "Region", "Province", "City", "TAI", "SITE", "CELL", "Authentication_Mode",
                        "Release_Cause_Code", "Abnormal_Release_Information", "Abnormal_Release_Cause",
                        "First_Release_NE_Type")
                .agg(
//                      Success Count
                        sum("SuccessCount").alias("SuccessCount"),
//                      Fail Count
                        sum("FailCount").alias("FailCount"),
//                      Total Count
                        sum(col("SuccessCount").plus(col("FailCount"))).alias("TotalCount"),
//                      Delay ms
                        sum(when(col("Service_Status").equalTo(0), col("End_Time_ms")).otherwise(0)).alias("Delay_ms"),
//                      Registration_Request_Valid_Duration_s
                        sum("Registration_Request_Valid_Duration_s").alias("Registration_Request_Valid_Duration_s"),
//                      Registration_Response_Valid_Duration_s
                        sum("Registration_Response_Valid_Duration_s").alias("Registration_Response_Valid_Duration_s")
                );
        return ds;
    }
}