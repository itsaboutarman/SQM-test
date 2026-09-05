
package ir.mohaymen.sqm.app.ims.voltevoicequality.summary.temp;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class VoLTEVoiceQualityTempSummary extends Summary {

    public VoLTEVoiceQualityTempSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Interface"
                ,"Country" , "Region" , "Province","City",
                "One-way_Identifier","SBC_Media", "Uplink_RTP_Packets","Uplink_RTP_Packets_Lost"
                ,"Downlink_RTP_Packets","Downlink_RTP_Packets_Lost",
                "Uplink_Integrated_MOS","Downlink_Integrated_MOS"
                ,"Uplink_RTCP_One-way_Length","Downlink_RTCP_One-way_Length",
                "Uplink_RTP_One-way_Length","Downlink_RTP_One-way_Length"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        ds = ds.withColumn("A_SBC",lit(null))
                .withColumn("P_CSCF",lit((null)))
                .withColumn("I_CSCF",lit((null)))
                .withColumn("S_CSCF",lit((null)))
                .withColumn("HSS",lit((null)))
                .withColumn("AS",lit((null)))
                .withColumn("MGCF",lit((null)))
                .withColumn("MRFC",lit((null)))
                .withColumn("eMSC",lit((null)))
                .withColumn("ATCF",lit((null)))
                .withColumn("MSC",lit((null)))
                .withColumn("Access_Type",lit((null)));

        ds = ds.withColumnRenamed("One-way_Identifier","One_way_Identifier")
                .withColumnRenamed("Interface","Interface_Type");


        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = aggregateFunc(ds);

        ds = generateKeyHash(ds, new String[]{"Interface_Type","A_SBC","P_CSCF","I_CSCF","S_CSCF","HSS",
                "AS","MGCF","MRFC","eMSC","ATCF","MSC","Access_Type","Country" , "Region" , "Province","City",
                "One_way_Identifier","SBC_Media"});

        System.out.println("output schema:");
        ds.printSchema();

        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Interface_Type","A_SBC","P_CSCF","I_CSCF","S_CSCF","HSS",
                        "AS","MGCF","MRFC","eMSC","ATCF","MSC","Access_Type"
                        ,"Country" , "Region" , "Province","City",
                        "One_way_Identifier","SBC_Media")
                .agg(
                        sum("Uplink_RTP_Packets").alias("Uplink_RTP_Packets"),
                        sum("Uplink_RTP_Packets_Lost").alias("Uplink_RTP_Packets_Lost"),
                        sum("Downlink_RTP_Packets").alias("Downlink_RTP_Packets"),
                        sum("Downlink_RTP_Packets_Lost").alias("Downlink_RTP_Packets_Lost"),
                        sum("Uplink_Integrated_MOS").alias("Uplink_MOS"),
                        sum("Downlink_Integrated_MOS").alias("Downlink_MOS"),
                        count(lit(1)).alias("Uplink_MOS_Count"),
                        count(lit(1)).alias("Downlink_MOS_Count"),
                        sum(when(col("One_way_Identifier").equalTo(3) ,1).otherwise(0)).alias("Both_Oneway"),
                        sum(when(col("One_way_Identifier").equalTo(2) ,1).otherwise(0)).alias("Downlink_Oneway"),
                        sum(when(col("One_way_Identifier").equalTo(1) ,1).otherwise(0)).alias("Uplink_Oneway"),
                        sum(when(col("One_way_Identifier").isin(0,1,2,3) ,1).otherwise(0)).alias("Oneway_Count"),
                        sum("Downlink_RTCP_One-way_Length").alias("Downlink_RTCP_One_way_Length"),
                        sum("Uplink_RTCP_One-way_Length").alias("Uplink_RTCP_One_way_Length"),
                        sum("Uplink_RTP_One-way_Length").alias("Uplink_RTP_One_way_Length"),
                        sum("Downlink_RTP_One-way_Length").alias("Downlink_RTP_One_way_Length")
                );
        return ds;
    }
}

