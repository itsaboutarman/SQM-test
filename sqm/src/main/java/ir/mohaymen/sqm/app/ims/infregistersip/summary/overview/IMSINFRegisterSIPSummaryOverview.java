package ir.mohaymen.sqm.app.ims.infregistersip.summary.overview;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class IMSINFRegisterSIPSummaryOverview extends Summary {

    public IMSINFRegisterSIPSummaryOverview(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }
    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Table_id", "End_Time_ms", "Registration_Type", "Service_Status",
                "Interface_Type", "Source_NE_Type","Destination_NE_Type","Access_Type", "Country", "RegionName", "ProvinceName",
                "CityName", "Authentication_Mode", "Release_Cause_Code");
    }
    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds =ds.filter(col("RECORD_DATE").gt("2026-01-11 12:30:00"));
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
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
                "Interface_Type", "Source_NE_Type", "Destination_NE_Type","Access_Type", "Country", "Region", "Province",
                "City", "Authentication_Mode", "Release_Cause_Code"});

        System.out.println("output schema:");
        ds.printSchema();
        
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Table_id", "Registration_Type", "Service_Status",
                        "Interface_Type", "Source_NE_Type", "Destination_NE_Type","Access_Type", "Country", "Region", "Province",
                        "City", "Authentication_Mode", "Release_Cause_Code")
                .agg(
                        sum("SuccessCount").alias("SuccessCount"),
                        sum("FailCount").alias("FailCount"),
                        sum(col("SuccessCount").plus(col("FailCount"))).alias("TotalCount"),
                        sum(when(col("Service_Status").equalTo(0),
                                col("End_Time_ms")).otherwise(0)
                        ).alias("Delay_ms")
                );
        return ds;
    }
}
