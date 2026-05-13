package ir.mohaymen.sqm.app.ims.infcallsip.summary.callsummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
@Job
public class IMSINFCallSIPSummary extends Summary {
    public IMSINFCallSIPSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Start_Time_s","Start_Time_ms", "IMEI","Session_id",
                "Table_id","Service_Type","Service_Status","Interface_Type",
                "Source_NE_Type","Destination_NE_Type","Calling_or_Called_Party_Indicator","Brand","Model","Type",
                "Technology","OS","Access_Type","Country","RegionName","ProvinceName","CityName","SITE","CELL",
                "Session_Interruption_Flag", "Early_Release_Flag","Release_Cause_Code","TAI","`Q_850_Failure_Cause_Code`",
                "Abnormal_Release_Information", "Abnormal_Release_Cause","First_Failure_NE_Type","UE_Handover_Capability_Flag",
                "Centrex_Call_Type","Abnormal_Play_Tone_Flag","End_Time_ms","Call_Ringing_Time",
                "Call_Answer_Time","Call_Duration_s","Call_Release_Time","183_Delay","PRACK_Delay","PRACK_200_OK_Delay");

        // return ds.withColumn("newcolumn",
        //                 functions.from_json(functions.col("value"),
        //                         functions.lit(Schema.getSchema().toDDL())))
        //         .select("newcolumn.*");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        // start time less than year 2026
        // ds =ds.filter(col("RECORD_DATE").gt("2025-12-29 14:00:00"));

        ds = ds.withColumn("RECORD_DATE" , window(col("RECORD_DATE") ,"5 minutes").getField("start"))
                .withColumn("TotalCount" , when(col("Service_Status").isin(0,1,3) ,1).otherwise(0))
                .withColumn("SuccessCount" ,when(col("Service_Status").isin(0,3),1).otherwise(0))
                .withColumn("FailCount" ,when(col("Service_Status").equalTo(1),1).otherwise(0) );
        ds = ds.withColumnRenamed("RegionName", "Region")
                .withColumnRenamed("ProvinceName", "Province")
                .withColumn("Q_850_Failure_Cause_Code", col("`Q_850_Failure_Cause_Code`"))
                .withColumnRenamed("CityName", "City");

        ds = aggregateFunc(ds);

        ds = generateKeyHash(ds, new String[]{"Table_id","Service_Type","Service_Status",
                "Interface_Type", "Source_NE_Type","Destination_NE_Type",
                "Calling_or_Called_Party_Indicator","Brand","Model","Type",
                "Technology","OS","Access_Type","Country","Region","Province",
                "City","SITE","CELL", "Session_Interruption_Flag", "Early_Release_Flag",
                "Release_Cause_Code","TAI","Q_850_Failure_Cause_Code", "Abnormal_Release_Information",
                "Abnormal_Release_Cause","First_Failure_NE_Type","UE_Handover_Capability_Flag",
                "Centrex_Call_Type","Abnormal_Play_Tone_Flag"});
                
        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds){

        return ds.withWatermark("RECORD_DATE", "600 minutes")
                .groupBy("Table_id","RECORD_DATE","Service_Type","Service_Status","Interface_Type",
                "Source_NE_Type","Destination_NE_Type","Calling_or_Called_Party_Indicator","Brand","Model","Type",
                "Technology","OS","Access_Type","Country","Region","Province","City","SITE","CELL",
                "Session_Interruption_Flag", "Early_Release_Flag","Release_Cause_Code","TAI","Q_850_Failure_Cause_Code",
                "Abnormal_Release_Information", "Abnormal_Release_Cause","First_Failure_NE_Type","UE_Handover_Capability_Flag",
                "Centrex_Call_Type","Abnormal_Play_Tone_Flag"
        ).agg(
                sum("TotalCount").alias("TotalCount"),
                sum("SuccessCount").alias("SuccessCount"),
                sum("FailCount").alias("FailCount"),
                sum("End_Time_ms").alias("Delay_ms"),
                sum(abs(col("Call_Ringing_Time").minus(col("Call_Answer_Time")))).alias("Call_Setup_Time_Delay"),
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
