package ir.mohaymen.sqm.app.gtpv2.sourcing;

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

@Job
public class GTPV2Sourcing extends Sourcing {

    public GTPV2Sourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");
        Dataset<Row> mmeRef = references.get("mmeRef");
        Dataset<Row> mccMncRef = references.get("mccMncRef");
        Dataset<Row> cellRef = references.get("cellRef");

        ds = ds.where(col("ROAM_DIRECTION").isNotNull());

        Dataset<Row> mccMncRefVisited = mccMncRef.select("Country" ,"MCC","MNC" ,"Operator", "Roaming_Type");
        mccMncRefVisited = mccMncRefVisited.withColumnRenamed("Country", "Visited_Country")
                .withColumnRenamed("Operator" ,"Visited_Operator")
                .withColumnRenamed("Roaming_Type","Visited_Roaming_Type")
                .withColumnRenamed("MCC", "Visited_MCC_ref")
                .withColumnRenamed("MNC", "Visited_MNC_ref");

        mccMncRefVisited.persist();

        // ------------------------------- Network & Roaming Information -------------------------------
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

        // ------------------------------- NE Information -------------------------------
        Dataset<Row> sgwRef = sgw_pgw_ref.select("NE_Name", "SGW_PGW_IP_Address")
                .withColumnRenamed("NE_Name", "SGW")
                .withColumnRenamed("SGW_PGW_IP_Address", "SGW_SIG_IP_Ref");

        Dataset<Row> pgwRef = sgw_pgw_ref.select("NE_Name", "SGW_PGW_IP_Address")
                .withColumnRenamed("NE_Name", "PGW")
                .withColumnRenamed("SGW_PGW_IP_Address", "PGW_SIG_IP_Ref");

        sgwRef.persist();
        pgwRef.persist();

        // ------------------------------- MME -------------------------------
        mmeRef = mmeRef.select("MME_Name", "MME_SIG_IP")
                .withColumnRenamed("MME_SIG_IP", "MME_SIG_IP_Ref");
        mmeRef.persist();

        // ------------------------------- CELL Information -------------------------------
        cellRef = cellRef.select("Country", "Region", "Province", "City", "SITE", "CELL",
                        "AccessLocationInfo", "TAI")
                .withColumn("TAI_Upper", upper(col("TAI")))
                .withColumn("AccessLocationInfo_Upper", upper(col("AccessLocationInfo")))
                .drop("TAI")
                .drop("AccessLocationInfo");
        cellRef.persist();


        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("TRANS_REQ_TIME_SEC")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // ------------------------------- CONDITION: Device -------------------------------
        ds = ds.withColumn("DeviceTAC", substring(lpad(col("IMEI"), 15, "0"), 1, 8));
        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));

        // ------------------------------- CONDITION: NE -------------------------------
        Column sgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGW_SIG_IP").equalTo(col("sgwRef.SGW_SIG_IP_Ref"))));
        Column pgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("PGW_SIG_IP").equalTo(col("pgwRef.PGW_SIG_IP_Ref"))));

        // ------------------------------- CONDITION: MME -------------------------------
        Column mmeCond = ds.col("MME_SIG_IP").equalTo(col("mme_ref.MME_SIG_IP_Ref"));

        // ------------------------------- CONDITION: Network & Roaming Information (MCC-MNC) -------------------------------
        Column mccMncCondVisited = ds.col("MCC").cast(DataTypes.IntegerType)
                .equalTo(mccMncRefVisited.col("Visited_MCC_ref").cast(DataTypes.IntegerType))
                .and(ds.col("MNC").cast(DataTypes.IntegerType).
                        equalTo(mccMncRefVisited.col("Visited_MNC_ref").cast(DataTypes.IntegerType)));

        Column mccMncCond =
                col("HOMEMCC").cast(DataTypes.IntegerType)
                        .equalTo(col("MCC_MNC_ref.MCC_Ref").cast(DataTypes.IntegerType))
                        .and(
                                col("HOMEMNC").cast(DataTypes.IntegerType)
                                        .equalTo(col("MCC_MNC_ref.MNC_Ref").cast(DataTypes.IntegerType))
                        );

        // ------------------------------- CONDITION: CELL Information -------------------------------
        Column cellCond = ds.col("SAI_CGI_ECGI").isNotNull()
                .and((ds.col("RAT").equalTo(lit(6))))
                .and(
                        upper(col("RAI_TAI")).equalTo(col("cell_ref.TAI_Upper")))
                .and(
                        upper(col("SAI_CGI_ECGI")).equalTo(col("cell_ref.AccessLocationInfo_Upper"))
                );

        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(mmeRef.as("mme_ref")), mmeCond, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgwCondRat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgwCondRat6, "leftouter")
                .join(broadcast(mccMncRefVisited), mccMncCondVisited,"leftouter")
                .join(broadcast(mccMncRef.as("MCC_MNC_ref")), mccMncCond, "leftouter")
                .join(broadcast(cellRef.as("cell_ref")), cellCond, "leftouter")

                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))

                .withColumn("MME_Name", coalesce(mmeRef.col("MME_Name"), lit("Unknown")))

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
                .drop("Visited_MNC_ref")
                .drop("SGW_SIG_IP_Ref")
                .drop("PGW_SIG_IP_Ref")
                .drop("TAI_Upper")
                .drop("AccessLocationInfo_Upper")
                .withColumnRenamed("MME_SIG_IP", "MME_IP_Address");
        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }


}
