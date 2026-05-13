package ir.mohaymen.sqm.app.ims.infregistersip.summary.qualitysummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class IMSINFRegisterSIPQualitySummary extends Summary {
    public IMSINFRegisterSIPQualitySummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country", "RegionName", "ProvinceName",
                "CityName", "CELL", "SITE", "Authentication_Mode", "Release_Cause_Code", "Abnormal_Release_Information",
                "Abnormal_Release_Cause", "First_Release_NE_Type", "First_Release_NE_Name", "Calc_Time_s",
                "Start_Time_s", "Start_Time_ms", "IMEI", "Session_id"
        );
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = ds.withColumn("FailCount" ,when(col("Service_Status").equalTo(1),1).otherwise(0))
                .withColumn("SuccessCount" , when(col("Service_Status").equalTo(0) ,1).otherwise(0))
                .withColumn("TotalCount" ,when(col("Service_Status").isin(0, 1),1).otherwise(0));

        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{
                "Table_id", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country", "Region", "Province",
                "City", "CELL", "SITE", "Authentication_Mode", "Release_Cause_Code", "Abnormal_Release_Information",
                "Abnormal_Release_Cause", "First_Release_NE_Type", "First_Release_NE_Name"});
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Table_id", "Registration_Type", "Service_Status",
                        "Interface_Type", "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                        "Brand", "Model", "Type", "Technology", "OS", "Access_Type", "Country", "Region", "Province",
                        "City", "CELL", "SITE", "Authentication_Mode", "Release_Cause_Code", "Abnormal_Release_Information",
                        "Abnormal_Release_Cause", "First_Release_NE_Type", "First_Release_NE_Name")
                .agg(
                        sum("SuccessCount").alias("SuccessCount"),
                        sum("FailCount").alias("FailCount"),
                        sum("TotalCount").alias("TotalCount"),
                        sum(when(col("Service_Status").equalTo(0),
                                (col("Calc_Time_s").minus(col("Start_Time_s")))).otherwise(0)
                        ).alias("Delay_ms")
                );
        return ds;
    }
}