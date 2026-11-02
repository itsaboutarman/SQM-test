
package ir.mohaymen.sqm.app.ims.voltevoicequality.summary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class VoLTEVoiceQualityMuteSummary extends Summary {

    public VoLTEVoiceQualityMuteSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Country" , "Region" ,"Province","City", "One-way_Identifier");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = aggregateFunc(ds);

        ds = generateKeyHash(ds, new String[]{"Country" , "Region" ,"Province","City"});

        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Country" , "Region" ,"Province","City")
                .agg(
                        sum(when(col("One-way_Identifier").equalTo(1) , 1).otherwise(0)).alias("Uplink_OneWay_Count"),
                        sum(when(col("One-way_Identifier").equalTo(2) , 1).otherwise(0)).alias("Downlink_OneWay_Count"),
                        sum(when(col("One-way_Identifier").equalTo(3) , 1).otherwise(0)).alias("Both_OneWay_Count"),
                        sum(when(col("One-way_Identifier").isin(0,1,2,3) , 1).otherwise(0)).alias("Total_Call_Count")
                );
        return ds;
    }
}

