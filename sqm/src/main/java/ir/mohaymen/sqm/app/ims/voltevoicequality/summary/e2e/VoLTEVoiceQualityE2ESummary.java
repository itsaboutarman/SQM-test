package ir.mohaymen.sqm.app.ims.voltevoicequality.summary.e2e;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class VoLTEVoiceQualityE2ESummary extends Summary {

    public VoLTEVoiceQualityE2ESummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Country" , "Region" ,"Province","City","SBC_Media",
                "One-way_Identifier","Uplink_RTP_Packets" ,"Uplink_RTP_Packets_Lost" ,"Downlink_RTP_Packets",
                "Downlink_RTP_Packets_Lost","Uplink_Integrated_MOS","Downlink_Integrated_MOS");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = aggregateFunc(ds);

        ds = generateKeyHash(ds, new String[]{"Country" , "Region" ,"Province","City" ,"SBC_Media"});

        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Country" , "Region" ,"Province","City" , "SBC_Media")
                .agg(
                        sum("Uplink_RTP_Packets").alias("Uplink_RTP_Packets"),
                        sum("Uplink_RTP_Packets_Lost").alias("Uplink_RTP_Packets_Lost"),
                        sum("Downlink_RTP_Packets").alias("Downlink_RTP_Packets"),
                        sum("Downlink_RTP_Packets_Lost").alias("Downlink_RTP_Packets_Lost"),
                        sum("Uplink_Integrated_MOS").alias("Uplink_MOS"),
                        sum("Downlink_Integrated_MOS").alias("Downlink_MOS"),
                        sum(when(col("Uplink_Integrated_MOS").between(1,5) ,1).otherwise(0)).alias("Uplink_MOS_count"),
                        sum(when(col("Downlink_Integrated_MOS").between(1,5) ,1).otherwise(0)).alias("Downlink_MOS_count"),
                        sum(when(col("One-way_Identifier")
                                .equalTo(1) , 1).otherwise(0)).alias("Uplink_OneWay"),
                        sum(when(col("One-way_Identifier")
                                .equalTo(2) , 1).otherwise(0)).alias("Downlink_OneWay"),
                        sum(when(col("One-way_Identifier")
                                .equalTo(3) , 1).otherwise(0)).alias("Both_OneWay"),
                        sum(when(col("One-way_Identifier")
                                .isin(0,1,2,3) , 1).otherwise(0)).alias("OneWay_Count")

                );
        return ds;
    }
}
