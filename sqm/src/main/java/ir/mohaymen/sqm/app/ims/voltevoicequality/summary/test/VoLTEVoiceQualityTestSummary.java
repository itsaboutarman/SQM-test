package ir.mohaymen.sqm.app.ims.voltevoicequality.summary.test;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

@Job
public class VoLTEVoiceQualityTestSummary extends Summary {

    public VoLTEVoiceQualityTestSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Interface", "CELL_Name_First", "SITE_NAME_First", "City_First",
                "Province_First", "Region_First", "Country_First", "CELL_Name_Last", "SITE_NAME_Last", "City_Last",
                "Province_Last", "Region_Last", "Country_Last", "One-way_Identifier", "SBC_Media",
                "Uplink_RTP_Packets", "Uplink_RTP_Packets_Lost", "Downlink_RTP_Packets", "Downlink_RTP_Packets_Lost",
                "Uplink_Integrated_MOS", "Downlink_Integrated_MOS", "Downlink_RTCP_One-way_Length",
                "Uplink_RTCP_One-way_Length", "Uplink_RTP_One-way_Length", "Downlink_RTP_One-way_Length");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = ds.withColumnRenamed("One-way_Identifier", "One_way_Identifier")
                .withColumnRenamed("Downlink_RTCP_One-way_Length", "Downlink_RTCP_One_way_Length")
                .withColumnRenamed("Uplink_RTCP_One-way_Length", "Uplink_RTCP_One_way_Length")
                .withColumnRenamed("Uplink_RTP_One-way_Length", "Uplink_RTP_One_way_Length")
                .withColumnRenamed("Downlink_RTP_One-way_Length", "Downlink_RTP_One_way_Length")
                .withColumnRenamed("Interface", "Interface_Type")

                .withColumn("A_SBC", lit(null).cast(DataTypes.StringType))
                .withColumn("P_CSCF", lit(null).cast(DataTypes.StringType))
                .withColumn("I_CSCF", lit(null).cast(DataTypes.StringType))
                .withColumn("S_CSCF", lit(null).cast(DataTypes.StringType))
                .withColumn("HSS", lit(null).cast(DataTypes.StringType))
                .withColumn("AS", lit(null).cast(DataTypes.StringType))
                .withColumn("MGCF", lit(null).cast(DataTypes.StringType))
                .withColumn("MRFC", lit(null).cast(DataTypes.StringType))
                .withColumn("eMSC", lit(null).cast(DataTypes.StringType))
                .withColumn("ATCF", lit(null).cast(DataTypes.StringType))
                .withColumn("MSC", lit(null).cast(DataTypes.StringType))
                .withColumn("Access_Type", lit(null).cast(DataTypes.StringType));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Interface_Type", "A_SBC", "P_CSCF", "I_CSCF", "S_CSCF", "HSS", "AS", "MGCF",
                "MRFC", "eMSC", "ATCF", "MSC", "Access_Type", "CELL_Name_First", "SITE_NAME_First",
                "City_First", "Province_First", "Region_First", "Country_First", "CELL_Name_Last",
                "SITE_NAME_Last", "City_Last", "Province_Last", "Region_Last", "Country_Last",
                "One_way_Identifier", "SBC_Media"});

        ds.printSchema();
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE", "Interface_Type", "A_SBC", "P_CSCF", "I_CSCF", "S_CSCF", "HSS", "AS", "MGCF",
                        "MRFC", "eMSC", "ATCF", "MSC", "Access_Type", "CELL_Name_First", "SITE_NAME_First",
                        "City_First", "Province_First", "Region_First", "Country_First", "CELL_Name_Last",
                        "SITE_NAME_Last", "City_Last", "Province_Last", "Region_Last", "Country_Last",
                        "One_way_Identifier", "SBC_Media")
                .agg(
                        sum("Uplink_RTP_Packets").alias("Uplink_RTP_Packets"),
                        sum("Uplink_RTP_Packets_Lost").alias("Uplink_RTP_Packets_Lost"),
                        sum("Downlink_RTP_Packets").alias("Downlink_RTP_Packets"),
                        sum("Downlink_RTP_Packets_Lost").alias("Downlink_RTP_Packets_Lost"),
                        sum("Uplink_Integrated_MOS").alias("Uplink_MOS"),
                        sum("Downlink_Integrated_MOS").alias("Downlink_MOS"),
                        sum(when(col("Uplink_Integrated_MOS").between(1,5) ,1).otherwise(0)).alias("Uplink_MOS_count"),
                        sum(when(col("Downlink_Integrated_MOS").between(1,5) ,1).otherwise(0)).alias("Downlink_MOS_count"),

                        sum(when(col("One_way_Identifier")
                                .equalTo(1) , 1).otherwise(0)).alias("Uplink_OneWay"),
                        sum(when(col("One_way_Identifier")
                                .equalTo(2) , 1).otherwise(0)).alias("Downlink_OneWay"),
                        sum(when(col("One_way_Identifier")
                                .equalTo(3) , 1).otherwise(0)).alias("Both_OneWay"),
                        sum(when(col("One_way_Identifier")
                                .isin(0,1,2,3) , 1).otherwise(0)).alias("OneWay_Count"),

                        sum("Downlink_RTCP_One_way_Length").alias("Downlink_RTCP_One_way_Length"),
                        sum("Uplink_RTCP_One_way_Length").alias("Uplink_RTCP_One_way_Length"),
                        sum("Uplink_RTP_One_way_Length").alias("Uplink_RTP_One_way_Length"),
                        sum("Downlink_RTP_One_way_Length").alias("Downlink_RTP_One_way_Length")
                );
        return ds;
    }
}
