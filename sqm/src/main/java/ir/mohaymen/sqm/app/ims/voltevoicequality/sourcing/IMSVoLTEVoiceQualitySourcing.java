package ir.mohaymen.sqm.app.ims.voltevoicequality.sourcing;

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
import static org.apache.spark.sql.functions.col;

@Job
public class IMSVoLTEVoiceQualitySourcing extends Sourcing {
    public IMSVoLTEVoiceQualitySourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> locationRefFisrt = references.get("locationRef");
        Dataset<Row> locationRefLast = references.get("locationRef");
        Dataset<Row> mediaRef0 = references.get("mediaRef");

        // ------------------------------- Location -------------------------------
        locationRefFisrt = locationRefFisrt.select("ECGI", "CELL_Name", "SITE_NAME", "City", "Province", "Region", "Country")
                .withColumnRenamed("ECGI", "ECGI_Ref_First")
                .withColumnRenamed("CELL_Name", "CELL_Name_First")
                .withColumnRenamed("SITE_NAME", "SITE_NAME_First")
                .withColumnRenamed("City", "City_First")
                .withColumnRenamed("Province", "Province_First")
                .withColumnRenamed("Region", "Region_First")
                .withColumnRenamed("Country", "Country_First");

        locationRefFisrt.persist();

        locationRefLast = locationRefLast.select("ECGI", "CELL_Name", "SITE_NAME", "City", "Province", "Region", "Country")
                .withColumnRenamed("ECGI", "ECGI_Ref_Last")
                .withColumnRenamed("CELL_Name", "CELL_Name_Last")
                .withColumnRenamed("SITE_NAME", "SITE_NAME_Last")
                .withColumnRenamed("City", "City_Last")
                .withColumnRenamed("Province", "Province_Last")
                .withColumnRenamed("Region", "Region_Last")
                .withColumnRenamed("Country", "Country_Last");

        locationRefLast.persist();

        // ------------------------------- Media -------------------------------
        mediaRef0 = mediaRef0.withColumnRenamed("Entity_Name" ,"SBC_Media0");
        Dataset<Row> mediaRef1 = mediaRef0.withColumnRenamed("IP","IP1")
                .withColumnRenamed("SBC_Media0", "SBC_Media1");

        mediaRef0.persist();
        mediaRef1.persist();

        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));

        // ------------------------------- CONDITION: Location -------------------------------
        Column locationCondFirst = ds.col("First_ECGI").isNotNull()
                .and(length(ds.col("First_ECGI")).equalTo(12))
                .and(ds.col("First_ECGI").equalTo(col("locationRefFirst.ECGI_Ref_First")));

        Column locationCondLast = ds.col("Last_ECGI").isNotNull()
                .and(length(ds.col("Last_ECGI")).equalTo(12))
                .and(ds.col("Last_ECGI").equalTo(col("locationRefLast.ECGI_Ref_Last")));

        // ------------------------------- CONDITION: Media -------------------------------
        Column mediaCond0 = ds.col("Call_Identity").equalTo(0).and(ds.col("Destination_IP").equalTo(mediaRef0.col("IP")));
        Column mediaCond1 = ds.col("Call_Identity").equalTo(1).and(ds.col("Source_IP").equalTo(mediaRef1.col("IP1")));


        ds = ds.join(broadcast(locationRefFisrt).alias("locationRefFirst"), locationCondFirst, "leftouter")
                .join(broadcast(locationRefLast).alias("locationRefLast"), locationCondLast, "leftouter")
                .join(broadcast(mediaRef0).alias("mediaRef0") ,mediaCond0 , "leftouter")
                .join(broadcast(mediaRef1).alias("mediaRef1") ,mediaCond1 , "leftouter")

                .withColumn("SBC_Media" , coalesce(mediaRef0.col("SBC_Media0") , mediaRef1.col("SBC_Media1") , lit("unknown")))
                .drop("ECGI_Ref_First")
                .drop("ECGI_Ref_Last")
                .drop("SBC_Media0")
                .drop("SBC_Media1")
                .drop("IP")
                .drop("IP1");
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}
