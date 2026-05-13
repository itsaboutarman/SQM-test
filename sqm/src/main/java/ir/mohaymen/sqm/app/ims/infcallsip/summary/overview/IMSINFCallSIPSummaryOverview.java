package ir.mohaymen.sqm.app.ims.infcallsip.summary.overview;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
@Job
public class IMSINFCallSIPSummaryOverview extends Summary {
    public IMSINFCallSIPSummaryOverview(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Table_id", "Service_Type", "Service_Status", "Interface_Type",
                "Source_NE_Type",  "Destination_NE_Type", "Calling_or_Called_Party_Indicator", "Access_Type",
                "Country", "RegionName", "ProvinceName", "CityName", "Session_Interruption_Flag", "Release_Cause_Code",
                "End_Time_ms","Call_Duration_s","Call_Ringing_Time","Call_Answer_Time", "Call_Release_Time",
                "183_Delay", "PRACK_Delay", "PRACK_200_OK_Delay");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds =ds.filter(col("RECORD_DATE").gt("2026-01-11 12:30:00"));
        ds = ds.withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"5 minutes").getField("start"))
                .withColumn("TotalCount" , when(col("Service_Status").isin(0, 1, 3) ,1).otherwise(0))
                .withColumn("SuccessCount" ,when(col("Service_Status").isin(0, 3),1).otherwise(0))
                .withColumn("FailCount" ,when(col("Service_Status").equalTo(1),1).otherwise(0) );

        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Table_id", "Service_Type", "Service_Status", "Interface_Type", "Source_NE_Type",
                "Destination_NE_Type", "Calling_or_Called_Party_Indicator", "Access_Type", "Country", "Region", "Province", "City",
                "Session_Interruption_Flag", "Release_Cause_Code"});
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds){

        return ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Table_id", "Service_Type", "Service_Status", "Interface_Type", "Source_NE_Type",
                        "Destination_NE_Type", "Calling_or_Called_Party_Indicator", "Access_Type", "Country", "Region", "Province",
                        "City", "Session_Interruption_Flag", "Release_Cause_Code"
        ).agg(
                sum("TotalCount").alias("TotalCount"),
                sum("SuccessCount").alias("SuccessCount"),
                sum("FailCount").alias("FailCount"),
                sum("End_Time_ms").alias("Delay_ms"),
                sum("Call_Duration_s").alias("Call_Duration"),
                sum("Call_Ringing_Time").alias("Call_Ringing_Time"),
                sum("Call_Answer_Time").alias("Call_Answer_Time"),
                sum("Call_Release_Time").alias("Call_Release_Time"),
                sum("183_Delay").alias("183_Delay"),
                sum("PRACK_Delay").alias("PRACK_Delay"),
                sum("PRACK_200_OK_Delay").alias("PRACK_200_OK_Delay")
        );
    }
}
