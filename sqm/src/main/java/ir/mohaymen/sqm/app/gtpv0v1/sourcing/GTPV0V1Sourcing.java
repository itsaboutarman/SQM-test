package ir.mohaymen.sqm.app.gtpv0v1.sourcing;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Map;

import static org.apache.spark.sql.functions.*;


public class GTPV0V1Sourcing extends Sourcing {

    public GTPV0V1Sourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> mccMncRef = references.get("mccMncRef");

        ds = ds.where(col("INTERFACEID").equalTo(1).and(col("ROAM_DIRECTION").isNotNull()));

        // ------------------------------- Network & Roaming Information -------------------------------
        Dataset<Row> mccMncRefVisited = mccMncRef.select("Country", "MCC", "MNC", "Operator", "Roaming_Type");
        mccMncRefVisited = mccMncRefVisited.withColumnRenamed("Country", "Visited_Country")
                .withColumnRenamed("Operator", "Visited_Operator")
                .withColumnRenamed("Roaming_Type", "Visited_Roaming_Type")
                .withColumnRenamed("MCC", "Visited_MCC_ref")
                .withColumnRenamed("MNC", "Visited_MNC_ref");
        mccMncRefVisited.persist();

        mccMncRef = mccMncRef.select("Region", "Country", "MCC", "MNC", "Operator", "Roaming_Type")
                .withColumnRenamed("Region", "Home_Region")
                .withColumnRenamed("Country", "Home_Country")
                .withColumnRenamed("Operator" , "Home_Operator")
                .withColumnRenamed("Roaming_Type", "Home_Roaming_Type")
                .withColumnRenamed("MNC", "MNC_Ref")
                .withColumnRenamed("MCC", "MCC_Ref");
        mccMncRef.persist();

        // ------------------------------- Device Reference -------------------------------
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"));
        deviceRef.persist();

        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("TRANS_REQ_TIME_SEC")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // ------------------------------- CONDITION: Device -------------------------------
        ds = ds.withColumn("DeviceTAC", substring(lpad(col("IMEI"), 15, "0"), 1, 8));
        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));

        // ------------------------------- CONDITION: Network & Roaming Information (MCC-MNC) -------------------------------
        Column mccMncCondVisited = ds.col("MCC").cast(DataTypes.IntegerType)
                .equalTo(mccMncRefVisited.col("Visited_MCC_ref").cast(DataTypes.IntegerType))
                .and(ds.col("MNC").cast(DataTypes.IntegerType).
                        equalTo(mccMncRefVisited.col("Visited_MNC_ref").cast(DataTypes.IntegerType)));

        Column mccMncCond = col("HOMEMCC").cast(DataTypes.IntegerType)
                        .equalTo(col("MCC_MNC_ref.MCC_Ref").cast(DataTypes.IntegerType))
                        .and(
                                col("HOMEMNC").cast(DataTypes.IntegerType)
                                        .equalTo(col("MCC_MNC_ref.MNC_Ref").cast(DataTypes.IntegerType))
                        );

        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(mccMncRefVisited), mccMncCondVisited,"leftouter")
                .join(broadcast(mccMncRef.as("MCC_MNC_ref")), mccMncCond, "leftouter")

                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))

                .withColumn("Visited_Country", coalesce(col("Visited_Country"),lit("Unknown")))
                .withColumn("Visited_Operator", coalesce(col("Visited_Operator"),lit("Unknown")))
                .withColumn("Visited_Roaming_Type", coalesce(col("Visited_Roaming_Type"),lit("Unknown")))
                .withColumn("Home_Country", coalesce(col("Home_Country"),lit("Unknown")))
                .withColumn("Home_Operator", coalesce(col("Home_Operator"),lit("Unknown")))
                .withColumn("Home_Roaming_Type", coalesce(col("Home_Roaming_Type"),lit("Unknown")));

        ds = ds.drop("TER_BRAND_NAME")
                .drop("TER_MODEL_NAME")
                .drop("TER_MODENAME")
                .drop("TER_TYPE_NAME_EN")
                .drop("TER_OS_TYPE_NAME")
                .drop("DeviceTAC")
                .drop("TAC_padded")
                .drop("MCC_ref")
                .drop("MNC_ref")
                .drop("Visited_MCC_ref")
                .drop("Visited_MNC_ref");

        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
