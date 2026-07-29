package ir.mohaymen.sqm.app.tdrmap.sourcing;

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
        Dataset<Row> mccMncRefVisited = references.get("mccMncRef");
        ds = ds.where(col("ROAM_DIRECTION").isNotNull());

        // ------------------------------- Network & Roaming Information -------------------------------
        mccMncRefVisited = mccMncRefVisited.select("Visited_Country", "Visited_Operator", "MSRN", "Visited_Roaming_Type");
        mccMncRefVisited = mccMncRefVisited.withColumnRenamed("MSRN", "MSRN_ref");
        mccMncRefVisited.persist();

        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("STARTTIME")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // ------------------------------- CONDITION: Network & Roaming Information (MCC-MNC) -------------------------------
        ds = ds.withColumn("Roaming_Prefix", concat(col("VISITCC"), col("VISITNDC")));

        Column prefixLength = length(ds.col("Roaming_Prefix"));

        Column mccMncCondVisited = ds.col("Roaming_Prefix").equalTo(mccMncRefVisited.col("MSRN_ref"));

        ds = ds.join(broadcast(mccMncRefVisited), mccMncCondVisited, "leftouter")
                .withColumn("Visited_Country", coalesce(col("Visited_Country"), lit("Unknown")))
                .withColumn("Visited_Operator", coalesce(col("Visited_Operator"), lit("Unknown")))
                .withColumn("Visited_Roaming_Type", coalesce(col("Visited_Roaming_Type"), lit("Unknown")))

                .drop("MSRN_ref")
                .drop("Roaming_Prefix");

        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
