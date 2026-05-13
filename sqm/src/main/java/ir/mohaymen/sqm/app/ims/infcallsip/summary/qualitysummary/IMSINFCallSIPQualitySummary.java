package ir.mohaymen.sqm.app.ims.infcallsip.summary.qualitysummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;
import static org.apache.spark.sql.functions.*;

@Job
public class IMSINFCallSIPQualitySummary extends Summary {
    public IMSINFCallSIPQualitySummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Table_id", "Service_Type", "Service_Status", "Interface_Type",
                "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                "Calling_or_Called_Party_Indicator", "Brand", "Model", "Type", "Technology", "OS", "Access_Type",
                "Country", "RegionName", "ProvinceName","CityName", "Session_Interruption_Flag", "SITE", "CELL",
                "Early_Release_Flag", "Release_Cause_Code", "Q_850_Failure_Cause_Code", "Abnormal_Release_Information",
                "Abnormal_Release_Cause", "First_Failure_NE_Type", "First_Release_NE_Name",
                "UE_Handover_Capability_Flag", "Centrex_Call_Type", "Abnormal_Play_Tone_Flag",
                "End_Time_ms", "Call_Duration_s", "Call_Ringing_Time", "Call_Answer_Time", "Call_Release_Time",
                "183_Delay", "PRACK_Delay", "PRACK_200_OK_Delay",
                "Start_Time_s", "Start_Time_ms", "IMEI", "Session_id");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"15 minutes").getField("start"))
                .withColumn("TotalCount" , when(col("Service_Status").isin(0, 1, 3) ,1).otherwise(0))
                .withColumn("SuccessCount" ,when(col("Service_Status").isin(0, 3),1).otherwise(0))
                .withColumn("FailCount" ,when(col("Service_Status").equalTo(1),1).otherwise(0));

        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Table_id", "Service_Type", "Service_Status", "Interface_Type",
                "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                "Calling_or_Called_Party_Indicator", "Brand", "Model", "Type", "Technology", "OS", "Access_Type",
                "Country", "Region", "Province","City", "Session_Interruption_Flag", "SITE", "CELL",
                "Early_Release_Flag", "Release_Cause_Code", "Q_850_Failure_Cause_Code", "Abnormal_Release_Information",
                "Abnormal_Release_Cause", "First_Failure_NE_Type", "First_Release_NE_Name",
                "UE_Handover_Capability_Flag", "Centrex_Call_Type", "Abnormal_Play_Tone_Flag"});
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds){
        return ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Table_id", "Service_Type", "Service_Status", "Interface_Type",
                        "Source_NE_Type", "Source_NE_Name", "Destination_NE_Type", "Destination_NE_Name",
                        "Calling_or_Called_Party_Indicator", "Brand", "Model", "Type", "Technology", "OS", "Access_Type",
                        "Country", "Region", "Province","City", "Session_Interruption_Flag", "SITE", "CELL",
                        "Early_Release_Flag", "Release_Cause_Code", "Q_850_Failure_Cause_Code", "Abnormal_Release_Information",
                        "Abnormal_Release_Cause", "First_Failure_NE_Type", "First_Release_NE_Name",
                        "UE_Handover_Capability_Flag", "Centrex_Call_Type", "Abnormal_Play_Tone_Flag"
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
