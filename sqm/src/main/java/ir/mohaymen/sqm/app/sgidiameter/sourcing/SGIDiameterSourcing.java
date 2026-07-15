package ir.mohaymen.sqm.app.sgidiameter.sourcing;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class SGIDiameterSourcing extends Sourcing {
    public SGIDiameterSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");

        // ------------------------------- Device Reference -------------------------------
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // ------------------------------- NE Information -------------------------------
        sgsnRef = sgsnRef.select("NE_Name", "Home_SGSN_Pool_Name", "Control_Plane_IP_Address")
                .withColumnRenamed("NE_Name", "SGSN")
                .withColumnRenamed("Home_SGSN_Pool_Name", "SGSN_POOL");

        ggsnRef = ggsnRef.select("NE_Name", "Control_Plane_IP_Address")
                .withColumnRenamed("NE_Name", "GGSN");

        Dataset<Row> sgwRef = sgw_pgw_ref.select("NE_Name", "SGW_PGW_IP_Address")
                .withColumnRenamed("NE_Name", "SGW");

        Dataset<Row> pgwRef = sgw_pgw_ref.select("NE_Name", "SGW_PGW_IP_Address")
                .withColumnRenamed("NE_Name", "PGW");

        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();


        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("TRANS_REQ_TIME_SEC")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // ------------------------------- Selected columns -------------------------------
        String[] cols = ds.columns();
        String[] addedCols = {"Brand", "Model", "Technology", "Type", "OS", "new_SGSN", "new_SGSN_POOL",
                "new_GGSN", "new_SGW", "new_PGW"};

        String[] selectedColumns = ArrayUtil.add(cols, addedCols);


        // ------------------------------- CONDITION: Device -------------------------------
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).lt(15), lpad(col("IMEI"), 15, "0"))
                        .otherwise(substring(col("IMEI"), 1, 8))
        );
        Column deviceCond = ds.col("DeviceTAC").equalTo(deviceRef.col("TAC_padded"));


        // ------------------------------- CONDITION: NE -------------------------------
        Column sgsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("SGSN_SGW_SIG_IP").equalTo(col("sgsnRef2.Control_Plane_IP_Address"))));
        Column ggsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("GGSN_PGW_SIG_IP").equalTo(col("ggsnRef2.Control_Plane_IP_Address"))));

        Column sgsnCondRat1 =((ds.col("RAT").equalTo(lit(1))).and(ds.col("SGSN_SGW_SIG_IP").equalTo(col("sgsnRef3.Control_Plane_IP_Address"))));
        Column ggsnCondRat1 = ((ds.col("RAT").equalTo(lit(1))).and(ds.col("GGSN_PGW_SIG_IP").equalTo(col("ggsnRef3.Control_Plane_IP_Address"))));

        Column sgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGSN_SGW_SIG_IP").equalTo(col("sgwRef.SGW_PGW_IP_Address"))));
        Column pgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("GGSN_PGW_SIG_IP").equalTo(col("pgwRef.SGW_PGW_IP_Address"))));


        ds = ds.join(broadcast(deviceRef.as("deviceRef")),deviceCond,"leftouter")
                .join(broadcast(sgsnRef.as("sgsnRef2")), sgsnCondRat2, "leftouter")
                .join(broadcast(sgsnRef.as("sgsnRef3")), sgsnCondRat1, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsnCondRat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsnCondRat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgwCondRat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgwCondRat6, "leftouter")

                // Device
                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME").cast(DataTypes.StringType), lit("Unknown")))

                // NE
                .withColumn("new_SGSN", coalesce(col("sgsnRef2.SGSN"), col("sgsnRef3.SGSN")))
                .withColumn("new_SGSN_POOL", coalesce(col("sgsnRef2.SGSN_POOL"), col("sgsnRef3.SGSN_POOL")))
                .withColumn("new_GGSN", coalesce(col("ggsnRef2.GGSN"), col("ggsnRef3.GGSN")))
                .withColumn("new_SGW", col("sgwRef.SGW"))
                .withColumn("new_PGW", col("pgwRef.PGW"))

                .selectExpr(selectedColumns)
                .withColumnRenamed("new_SGSN", "SGSN")
                .withColumnRenamed("new_SGSN_POOL", "SGSN_POOL")
                .withColumnRenamed("new_GGSN", "GGSN")
                .withColumnRenamed("new_SGW", "SGW")
                .withColumnRenamed("new_PGW", "PGW");

        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
