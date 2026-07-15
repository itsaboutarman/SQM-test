package ir.mohaymen.sqm.app.s6a.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class S6ASourcing extends Sourcing {

    public S6ASourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }


    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> mmeRef = references.get("mmeRef");
        Dataset<Row> hssRef = references.get("hssRef");
        Dataset<Row> mccMncRef = references.get("mccMncRef");

        ds = ds.where(col("INTERFACEID").equalTo(8));

        // Network & Roaming Information
        mccMncRef = mccMncRef.select("Region", "Country_SIM", "MCC", "MNC", "Operator", "Roaming_Type")
                .withColumnRenamed("MNC", "MNC_Ref")
                .withColumnRenamed("MCC", "MCC_Ref");
        mccMncRef.persist();

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"));
        deviceRef.persist();

        // MME
        mmeRef = mmeRef
                .select(col("MME_Name"),  explode(split(col("S6a_IP_Address"),";")).as("mme_ref_sig_ip"));
        mmeRef.persist();

        hssRef = hssRef.select(col("HSS_Name") , explode(split(col("HSS_IPs"),";")).as("HSS_IP"));
        hssRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("TRANS_REQ_TIME_SEC")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));

        //Get DeviceTAC
        ds = ds.withColumn("DeviceTAC",substring(lpad(col("IMEI"),15,"0"),1,8));

        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));
        Column mmeCond = ds.col("MME_SIG_IP").equalTo(col("mme_ref.mme_ref_sig_ip"));
        Column hssCond = ds.col("HSS_SIG_IP").equalTo(col("hss_ref.HSS_IP"));
        Column mccMncCond =
                col("HOMEMCC").cast(DataTypes.IntegerType)
                        .equalTo(col("MCC_MNC_ref.MCC_Ref").cast(DataTypes.IntegerType))
                        .and(
                                col("HOMEMNC").cast(DataTypes.IntegerType)
                                        .equalTo(col("MCC_MNC_ref.MNC_Ref").cast(DataTypes.IntegerType))
                        );

        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(mmeRef.as("mme_ref")), mmeCond, "leftouter")
                .join(broadcast(hssRef.as("hss_ref")), hssCond, "leftouter")
                .join(broadcast(mccMncRef.as("MCC_MNC_ref")), mccMncCond, "leftouter")

                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("MME_Name",coalesce(mmeRef.col("MME_Name"),lit("Unknown")))
                .withColumn("HSS_Name",coalesce(hssRef.col("HSS_Name"),lit("Unknown")))
                .withColumn("Country_SIM", coalesce(col("MCC_MNC_ref.Country_SIM"), lit("Unknown")))
                .withColumn("Operator", coalesce(col("MCC_MNC_ref.Operator"), lit("Unknown")));

        ds = ds.drop("mme_ref_sig_ip")
                .drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_padded")
                .drop("MNC_Ref")
                .drop("MCC_Ref")
                .withColumnRenamed("MME_SIG_IP", "MME_IP_Address");
        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }


}
