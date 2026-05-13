package ir.mohaymen.sqm.app.ims.infipmessagesip.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.app.ims.infcallsip.sourcing.Schema;
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

public class IMSINFIPMessageSIPSourcing extends Sourcing {
    public IMSINFIPMessageSIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(),"|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> locationRef = references.get("locationRef");

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // Location Information
        locationRef = locationRef
                .withColumn("TAC", lpad(hex(col("TAC").cast(DataTypes.IntegerType)) ,4 ,"0"))
                .withColumn("ENODBID", lpad(hex(col("ENODBID").cast(DataTypes.IntegerType)),5,"0"))
                .withColumn("CID", lpad(hex(col("CID").cast(DataTypes.IntegerType)),2,"0"))
                .withColumn("key", lower(concat(lit("43211"), col("TAC"), col("ENODBID"),
                        col("CID"))));
        locationRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);
        //Get DeviceTAC
        ds = ds.withColumn("DeviceTAC", lpad(replace(col("IMEI"),lit("-") ,lit("")) ,8 ,"0"));
        ds = ds.withColumn("location_key",concat(col("TAI"),(col("Access_Location_Info").substr(6,Integer.MAX_VALUE))));

        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));
        Column locationCond = ds.col("Access_Location_Info").isNotNull()
                .and(lower(ds.col("location_key")).equalTo(col("loc_ref.key")));

        ds = ds.drop("IMEI");
        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(locationRef.as("loc_ref")), locationCond, "leftouter")
                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("Country",locationRef.col("Country"))
                .withColumn("RegionName",locationRef.col("Region"))
                .withColumn("ProvinceName",locationRef.col("Province"))
                .withColumn("CityName",locationRef.col("City"))
                .withColumn("CELL",locationRef.col("CELL"))
                .withColumn("SITE",locationRef.col("SITE"));
        ds = ds.drop("loc_ref.key")
                .drop("loc_ref.ENODBID")
                .drop("loc_ref.CID")
                .drop("loc_ref.TAC")
                .drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_padded")
                .drop("location_key");
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}
