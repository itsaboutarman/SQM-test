package ir.mohaymen.sqm.app.ims.infcallsip.sourcing;
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
public class IMSINFCallSIPSourcing extends Sourcing {
    public IMSINFCallSIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
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
        Dataset<Row> location2g3gRef = references.get("2g3gLocationRef");

        // NE Information
        neRef = neRef.select("IP", "Entity_Name");

        Dataset<Row> neRef1 = neRef
                .withColumnRenamed("IP", "IP_ref1")
                .withColumnRenamed("Entity_Name", "Entity_Name_ref1");

        Dataset<Row> neRef2 = neRef
                .withColumnRenamed("IP", "IP_ref2")
                .withColumnRenamed("Entity_Name", "Entity_Name_ref2");

        Dataset<Row> neRef3 = neRef
                .withColumnRenamed("IP", "IP_ref3")
                .withColumnRenamed("Entity_Name", "Entity_Name_ref3");

        neRef1.persist();
        neRef2.persist();
        neRef3.persist();

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME" ,"TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_Padded", lpad(col("TAC"), 8 ,"0"))
                .withColumnRenamed("TAC" ,"DeviceTAC");

        deviceRef.persist();

        // Location Information
        locationRef = locationRef.withColumnRenamed("TAI", "locationTAI");
        locationRef = locationRef.drop("ENODeBID")
                .drop("CID")
                .withColumnRenamed("Country", "CountryName");
        locationRef.persist();

        location2g3gRef = location2g3gRef.withColumnRenamed("Country", "CountryName");
        location2g3gRef.persist();

        ds = ds.withColumn("CallTAC" , lpad(replace(col("IMEI"),lit("-"), lit("")), 8, "0"));

        // --------------- column conditions ---------------
        Column deviceCond = col("deviceRef.TAC_Padded").equalTo(col("call.CallTAC"));

        Column location4gCond = ds.col("Access_Type").isin(1, 2, 43)
                .and(col("Access_Location_Info").isNotNull())
                .and(upper(ds.col("Access_Location_Info")).equalTo(upper(col("locationRef.AccessLocationInfo"))))
                .and(upper(ds.col("TAI")).equalTo(upper(col("locationRef.locationTAI"))));

        Column baseCondition = col("Access_Type").isin(3, 4, 5, 44);
        ds = ds.withColumn("MCC", when(baseCondition, substring(col("Access_Location_Info"), 1, 3)))
                .withColumn("MNC", when(baseCondition, substring(col("Access_Location_Info"), 4, 2)))
                .withColumn("LAC", when(baseCondition, substring(col("Access_Location_Info"), 6, 4)))
                .withColumn("SAC_CC", when(baseCondition, substring(col("Access_Location_Info"), 10, 4)));

        Column location2g3gCond = ds.col("Access_Type").isin(3, 4, 5, 44)
                .and((ds.col("MCC")).equalTo(col("loc_2g_3g_ref.MCC")))
                .and((ds.col("MNC")).equalTo(col("loc_2g_3g_ref.MNC")))
                .and((ds.col("LAC")).equalTo(col("loc_2g_3g_ref.LAC")))
                .and((ds.col("SAC_CC")).equalTo(col("loc_2g_3g_ref.SAC_CC")));


        Column neCond1 = ds.col("Source_NE_IP_Address").equalTo(col("ne_ref1.IP_ref1"));
        Column neCond2 = ds.col("Destination_NE_IP_Address").equalTo(col("ne_ref2.IP_ref2"));
        Column neCond3 = ds.col("First_Failure_NE_IP_Address").equalTo(col("ne_ref3.IP_ref3"));

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));

//        ds = ds.withWatermark("RECORD_DATE", "24 hours");
                //.dropDuplicates("Session_id", "RECORD_DATE", "Start_Time_ms", "IMEI");

        ds = ds.alias("call")
                .join(broadcast(deviceRef).alias("deviceRef"), deviceCond, "leftouter")
                .join(broadcast(locationRef).alias("locationRef"), location4gCond, "leftouter")
                .join(broadcast(location2g3gRef.as("loc_2g_3g_ref")), location2g3gCond, "leftouter")
                .join(broadcast(neRef1.as("ne_ref1")), neCond1, "leftouter")
                .join(broadcast(neRef2.as("ne_ref2")), neCond2, "leftouter")
                .join(broadcast(neRef3.as("ne_ref3")), neCond3, "leftouter")

                .withColumnRenamed("Entity_Name_ref1", "Source_NE_Name")
                .withColumnRenamed("Entity_Name_ref2", "Destination_NE_Name")
                .withColumnRenamed("Entity_Name_ref3", "First_Release_NE_Name")

                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME") , lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME"), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("Country", coalesce(locationRef.col("CountryName"), location2g3gRef.col("CountryName")))
                .withColumn("RegionName",locationRef.col("Region"))
                .withColumn("ProvinceName",locationRef.col("Province"))
                .withColumn("CityName",locationRef.col("City"))
                .withColumn("CELL",locationRef.col("CELL"))
                .withColumn("Insert_Time" ,current_timestamp());

        ds = ds.drop("CallTAC")
                .drop("locationTAI")
                .drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_Padded")
                .drop("CID_HEX")
                .drop("IP_ref1")
                .drop("IP_ref2")
                .drop("IP_ref3")
                .drop("Region")
                .drop("City")
                .drop("CountryName")
                .drop("Province")
                .drop("MCC")
                .drop("MNC")
                .drop("LAC")
                .drop("SAC_CC");

        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}