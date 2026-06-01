package ir.mohaymen.sqm.app.ims.registersip.sourcing;

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

public class IMSRegisterSIPSourcing extends Sourcing {

    public IMSRegisterSIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        // -------------------- Reference Definition --------------------
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> neRef = references.get("neRef");

        // NE Reference
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

        Dataset<Row> neRef4 = neRef
                .withColumnRenamed("IP", "IP_ref4")
                .withColumnRenamed("Entity_Name", "Entity_Name_ref4");

        Dataset<Row> neRef5 = neRef
                .withColumnRenamed("IP", "IP_ref5")
                .withColumnRenamed("Entity_Name", "Entity_Name_ref5");

        neRef1.persist();
        neRef2.persist();
        neRef3.persist();
        neRef4.persist();
        neRef5.persist();

        // Device Reference
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME", "TER_TYPE_NAME_EN",
                        "TER_OS_TYPE_NAME").withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // Location Reference
        locationRef = locationRef.select("CityName", "ProvinceName", "RegionName", "CountryName", "Site_Name",
                "Cell_Name", "AccessLocationInfo", "locationTAI");
        locationRef.persist();

        // RECORD DATE
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // -------------------- Column Conditions --------------------
        // Device Cond
        ds = ds.withColumn("DeviceTAC", regexp_replace(col("IMEI"), "-", ""));
        ds = ds.withColumn("DeviceTAC",
                when(length(col("DeviceTAC")).lt(8),
                        rpad(col("DeviceTAC"), 8, "0"))
                        .otherwise(substring(col("DeviceTAC"), 0, 8))
        );
        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));

        // Location Cond
        Column locationCond = ds.col("Access_Location_Info").isNotNull()
                .and(upper(ds.col("Access_Location_Info")).equalTo(upper(col("loc_ref.AccessLocationInfo"))))
                .and(upper(ds.col("TAI")).equalTo(upper(col("loc_ref.locationTAI"))));

        // NE Cond
        Column neCond1 = ds.col("Access_NE_IP_Address").equalTo(col("ne_ref1.IP_ref1"));
        Column neCond2 = ds.col("P_CSCF_IP_Address").equalTo(col("ne_ref2.IP_ref2"));
        Column neCond3 = ds.col("I_CSCF_IP_Address").equalTo(col("ne_ref3.IP_ref3"));
        Column neCond4 = ds.col("S_CSCF_IP_Address").equalTo(col("ne_ref4.IP_ref4"));
        Column neCond5 = ds.col("AS_IP_Address").equalTo(col("ne_ref5.IP_ref5"));

        // -------------------- Left Joins & Column Definitions --------------------
        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(locationRef.as("loc_ref")), locationCond, "leftouter")
                .join(broadcast(neRef1.as("ne_ref1")), neCond1, "leftouter")
                .join(broadcast(neRef2.as("ne_ref2")), neCond2, "leftouter")
                .join(broadcast(neRef3.as("ne_ref3")), neCond3, "leftouter")
                .join(broadcast(neRef4.as("ne_ref4")), neCond4, "leftouter")
                .join(broadcast(neRef5.as("ne_ref5")), neCond5, "leftouter")
                // NE
                .withColumnRenamed("Entity_Name_ref1", "Access_NE_Name")
                .withColumnRenamed("Entity_Name_ref2", "P_CSCF_Name")
                .withColumnRenamed("Entity_Name_ref3", "I_CSCF_Name")
                .withColumnRenamed("Entity_Name_ref4", "S_CSCF_Name")
                .withColumnRenamed("Entity_Name_ref5", "AS_Name")

                // Device
                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                // Location
                .withColumn("Region", locationRef.col("RegionName"))
                .withColumn("Province",locationRef.col("ProvinceName"))
                .withColumn("City",locationRef.col("CityName"))
                .withColumn("Country",locationRef.col("CountryName"))
                // Insertion Time (Kafka Consume Time)
                .withColumn("Insert_Time", current_timestamp());

        // -------------------- Drop Unnecessary Columns --------------------
        // device
        ds = ds.drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("TAC_padded")
                .drop("DeviceTAC")
                // location
                .drop("locationTAI")
                .drop("RegionName")
                .drop("CityName")
                .drop("ProvinceName")
                .drop("CountryName")
                // NE
                .drop("IP_ref1")
                .drop("IP_ref2")
                .drop("IP_ref3")
                .drop("IP_ref4")
                .drop("IP_ref5");
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}