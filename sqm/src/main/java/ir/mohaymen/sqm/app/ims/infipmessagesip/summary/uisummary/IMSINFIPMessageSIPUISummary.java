package ir.mohaymen.sqm.app.ims.infipmessagesip.summary.uisummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

@Job
public class IMSINFIPMessageSIPUISummary extends Summary {
    public IMSINFIPMessageSIPUISummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","End_Time_ms",
                "Table_id", "Probe_ID", "Service_Status","Service_Type",
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type","Access_Type",
                "Release_Cause_Code", "Country", "Region", "Province", "City",
                "Brand", "Model", "Type", "Technology", "OS");
    }
    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));

        ds = ds.withColumn("SuccessCount", when(col("Service_Status").isin(0,3), 1).otherwise(0))
                .withColumn("FailCount", when(col("Service_Status").equalTo(1), 1).otherwise(0));
        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{
                "Table_id", "Probe_ID", "Service_Status","Service_Type",
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type","Access_Type",
                "Release_Cause_Code", "Country", "Region", "Province", "City",
                "Brand", "Model", "Type", "Technology", "OS"});
        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }
    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE",
                        "Table_id", "Probe_ID", "Service_Status","Service_Type",
                        "Interface_Type", "Source_NE_Type", "Destination_NE_Type","Access_Type",
                        "Release_Cause_Code", "Country", "Region", "Province", "City",
                        "Brand", "Model", "Type", "Technology", "OS")
                .agg(
//                       Success Count
                        sum("SuccessCount").alias("SuccessCount"),
//                      Fail Count
                        sum("FailCount").alias("FailCount"),
//                      Total Count
                        sum(col("SuccessCount").plus(col("FailCount"))).alias("TotalCount"),
//                      Delay ms
                        sum(col("End_Time_ms")).alias("Delay_ms")
                );
        return ds;
    }
}
