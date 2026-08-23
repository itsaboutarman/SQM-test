package ir.mohaymen.sqm.app.ims.infregistersip.sourcing;

import com.amazonaws.services.frauddetector.model.DataType;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.*;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class IMSINFRegisterSIPSourcing extends Sourcing {

    public IMSINFRegisterSIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> neRef = references.get("neRef");
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
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();
        locationRef = locationRef.withColumn("CID" , col("CID").cast(DataTypes.StringType));
        locationRef = locationRef.withColumn("ENODBID" , col("ENODeBID").cast(DataTypes.StringType));
        locationRef = locationRef.drop("ENODeBID");
        locationRef = locationRef.withColumnRenamed("TAI" ,"locationTAI")
                .withColumnRenamed("Country", "CountryName");

        location2g3gRef = location2g3gRef.withColumnRenamed("Country", "CountryName");
        location2g3gRef.persist();
        locationRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);


        ds = ds.withColumn("DeviceTAC", regexp_replace(col("IMEI"), "-", ""));
        ds = ds.withColumn("DeviceTAC",
                when(length(col("DeviceTAC")).lt(8),
                        rpad(col("DeviceTAC"), 8, "0"))
                        .otherwise(substring(col("DeviceTAC"), 0, 8))
        );


        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));

        // location conditions
        Column location4gCond = ds.col("Access_Type").isin(1, 2, 43)
                .and(col("Access_Location_Info").isNotNull())
                .and(upper(ds.col("Access_Location_Info")).equalTo(upper(col("loc4f_ref.AccessLocationInfo"))))
                .and(upper(ds.col("TAI")).equalTo(upper(col("loc4f_ref.locationTAI"))));


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
        Column neCond3 = ds.col("First_Release_NE_IP_Address").equalTo(col("ne_ref3.IP_ref3"));

        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(locationRef.as("loc4f_ref")), location4gCond, "leftouter")
                .join(broadcast(location2g3gRef.as("loc_2g_3g_ref")), location2g3gCond, "leftouter")
                .join(broadcast(neRef1.as("ne_ref1")), neCond1, "leftouter")
                .join(broadcast(neRef2.as("ne_ref2")), neCond2, "leftouter")
                .join(broadcast(neRef3.as("ne_ref3")), neCond3, "leftouter")
                .withColumnRenamed("Entity_Name_ref1", "Source_NE_Name")
                .withColumnRenamed("Entity_Name_ref2", "Destination_NE_Name")
                .withColumnRenamed("Entity_Name_ref3", "First_Release_NE_Name")

                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("RegionName", coalesce(locationRef.col("Region"), location2g3gRef.col("Region")))
                .withColumn("ProvinceName", coalesce(locationRef.col("Province"), location2g3gRef.col("Province")))
                .withColumn("CityName", coalesce(locationRef.col("City"), location2g3gRef.col("City")))
                .withColumn("Country", coalesce(locationRef.col("CountryName"), location2g3gRef.col("CountryName")))
                .withColumn("Insert_Time", current_timestamp());

        ds = ds.drop("TER_BRAND_NAME")
                .drop("locationTAI")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_padded")
                .drop("IP_ref1")
                .drop("IP_ref2")
                .drop("IP_ref3")
                .drop("Region")
                .drop("City")
                .drop("Province")
                .drop("MCC")
                .drop("MNC")
                .drop("LAC")
                .drop("CountryName")
                .drop("SAC_CC");
        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }


}
