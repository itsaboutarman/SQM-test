package ir.mohaymen.sqm.app.tdrmap.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class TDRMAPSourcing extends Sourcing {

    public TDRMAPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }


    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> sgsnRef = references.get("sgsnRef");
        Dataset<Row> transCauseRef = references.get("transCauseRef");

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"));
        deviceRef.persist();

        // SGSN Information
        sgsnRef = sgsnRef.select("NE_Name", "Home_SGSN_Pool_Name", "SGSNGT");
        sgsnRef.persist();

        //Transaction Cause
        transCauseRef = transCauseRef.withColumnRenamed("trans_cause", "ref_trans_cause");
        transCauseRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("STARTTIME")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));

        //Get DeviceTAC
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).equalTo(14),
                        concat(lit("0"), substring(col("IMEI"), 0, 7)))
                        .when(length(col("IMEI")).equalTo(13),
                                concat(lit("00"), substring(col("IMEI"), 0, 6)))
                        .otherwise(substring(col("IMEI"), 0, 8))
        );


        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));
        Column sgsnCond = ds.col("OGT").equalTo(col("sgsn_ref.SGSNGT"));
        Column transCauseCond = ds.col("CAUSE").equalTo(col("transcause_ref.ref_trans_cause"));


        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(sgsnRef.as("sgsn_ref")), sgsnCond, "leftouter")
                .join(broadcast(transCauseRef.as("transcause_ref")), transCauseCond, "leftouter")
                .withColumn("DeviceBrand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("DeviceModel", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("DeviceTechnology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("DeviceType", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("DeviceOS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("trans_cause_det", coalesce(col("transcause_ref.trans_cause_det"), col("CAUSE")));

        ds = ds.drop("SGSNGT")
                .drop("ref_trans_cause")
                .drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_padded")
                .withColumnRenamed("NE_Name", "SGSN_Name");

        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }


}
