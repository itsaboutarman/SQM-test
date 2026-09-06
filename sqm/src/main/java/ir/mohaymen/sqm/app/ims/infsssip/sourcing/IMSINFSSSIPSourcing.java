package ir.mohaymen.sqm.app.ims.infsssip.sourcing;
import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

@Job
public class IMSINFSSSIPSourcing extends Sourcing {
    public IMSINFSSSIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }
    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> neRef = references.get("neRef");
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> location2gRef = references.get("3gLocationRef");
        Dataset<Row> location3gRef = references.get("2gLocationRef");

        // NE Information
        neRef = neRef.select("IP", "Entity_Name");
        Dataset<Row> neRef1 = neRef
                .withColumnRenamed("IP", "IP_ref1")
                .withColumnRenamed("Entity_Name", "Source_NE_Name");

        Dataset<Row> neRef2 = neRef
                .withColumnRenamed("IP", "IP_ref2")
                .withColumnRenamed("Entity_Name", "Dest_NE_Name");

        neRef1.persist();
        neRef2.persist();

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME", "TER_TYPE_NAME_EN",
                        "TER_OS_TYPE_NAME")
                .withColumn("TAC_Padded", lpad(col("TAC"), 8 ,"0"));

        deviceRef.persist();

        // Location Information
        locationRef = locationRef.withColumnRenamed("TAI", "locationTAI");
        locationRef = locationRef
                .withColumnRenamed("Country", "CountryName")
                .withColumnRenamed("Region", "RegionName")
                .withColumnRenamed("Province", "ProvinceName")
                .withColumnRenamed("City", "CityName");
        locationRef.persist();

        location3gRef = location3gRef
                .withColumnRenamed("Country", "CountryName")
                .withColumnRenamed("Region", "RegionName")
                .withColumnRenamed("Province", "ProvinceName")
                .withColumnRenamed("City", "CityName");

        location2gRef = location2gRef
                .withColumnRenamed("Country", "CountryName")
                .withColumnRenamed("Region", "RegionName")
                .withColumnRenamed("Province", "ProvinceName")
                .withColumnRenamed("City", "CityName");
        location2gRef.persist();

        // --------------- device conditions ---------------
        ds = ds.withColumn("CallTAC", substring(lpad(col("IMEI"), 15, "0"), 1, 8));
        Column deviceCond = col("deviceRef.TAC_Padded").equalTo(ds.col("CallTAC"));

        // --------------- location conditions ---------------
        Column location4gCond = ds.col("Access_Type").isin(1, 2, 43)
                .and(col("Access_Location_Info").isNotNull())
                .and(upper(ds.col("Access_Location_Info")).equalTo(upper(col("locationRef.AccessLocationInfo"))))
                .and(upper(ds.col("TAI")).equalTo(upper(col("locationRef.locationTAI"))));

        Column location2gCond = ds.col("Access_Type").isin(3)
                .and((ds.col("Access_Location_Info")).equalTo(col("loc_2g_ref.AccessLocationInfo")));

        Column location3gCond = ds.col("Access_Type").isin(4, 5, 44)
                .and((ds.col("Access_Location_Info")).equalTo(col("loc_3g_ref.AccessLocationInfo")));

        // --------------- ne conditions ---------------
        Column neCond1 = ds.col("Source_NE_IP_Address").equalTo(col("ne_ref1.IP_ref1"));
        Column neCond2 = ds.col("Destination_NE_IP_Address").equalTo(col("ne_ref2.IP_ref2"));

        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        ds = ds.join(broadcast(deviceRef).alias("deviceRef"), deviceCond, "leftouter")
                .join(broadcast(locationRef).alias("locationRef"), location4gCond, "leftouter")
                .join(broadcast(location2gRef.as("loc_2g_ref")), location2gCond, "leftouter")
                .join(broadcast(location3gRef.as("loc_3g_ref")), location3gCond, "leftouter")
                .join(broadcast(neRef1.as("ne_ref1")), neCond1, "leftouter")
                .join(broadcast(neRef2.as("ne_ref2")), neCond2, "leftouter")

                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME") , lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME"), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME"), lit("Unknown")))

                // location
                .withColumn("Country", coalesce(locationRef.col("CountryName"), location3gRef.col("CountryName"),
                        location3gRef.col("CountryName"), lit("Unknown")))
                .withColumn("Region", coalesce(locationRef.col("RegionName"), location3gRef.col("RegionName"),
                        location3gRef.col("RegionName"), lit("Unknown")))
                .withColumn("Province", coalesce(locationRef.col("ProvinceName"), location3gRef.col("ProvinceName"),
                        location3gRef.col("ProvinceName"), lit("Unknown")))
                .withColumn("City", coalesce(locationRef.col("CityName"), location3gRef.col("CityName"),
                        location3gRef.col("CityName"), lit("CityName")))
                .withColumn("SITE", locationRef.col("SITE"))

                .withColumn("Insert_Time", current_timestamp());

        ds = ds.drop("locationTAI")

                .drop("CallTAC")
                .drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")

                .drop("TAC_Padded")

                .drop("IP_ref1")
                .drop("IP_ref2")

                .drop("RegionName")
                .drop("CityName")
                .drop("CountryName")
                .drop("ProvinceName")
                .drop("AccessLocationInfo");

        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}