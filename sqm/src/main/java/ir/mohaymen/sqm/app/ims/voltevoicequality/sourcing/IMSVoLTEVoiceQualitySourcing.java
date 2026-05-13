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
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> mediaRef0 = references.get("mediaRef");

        locationRef = locationRef.select("ECGI", "City", "Province", "Region", "Country", "ENodeB_Name", "CELL_Name", "LAT", "LNG")
                .withColumnRenamed("LAT", "Latitude")
                .withColumnRenamed("LNG", "Longitude")
                .withColumn("ECGI_1",
                        (split(col("ECGI"), "-").getItem(0).cast(DataTypes.IntegerType)))
                .withColumn("ECGI_2",
                        (split(col("ECGI"), "-").getItem(1).cast(DataTypes.IntegerType)))
                .withColumn("ECGI_3",
                        lower(hex(split(col("ECGI"), "-").getItem(2).cast(DataTypes.IntegerType))))
                .withColumn("ECGI_4",
                        lower(hex(split(col("ECGI"), "-").getItem(3).cast(DataTypes.IntegerType))))
                .drop("ECGI");

        locationRef.persist();

        mediaRef0 = mediaRef0.withColumnRenamed("Entity_Name" ,"SBC_Media0");
        Dataset<Row> mediaRef1 = mediaRef0.withColumnRenamed("IP","IP1")
        .withColumnRenamed("SBC_Media0","SBC_Media1");

        mediaRef0.persist();
        mediaRef1.persist();
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));

        ds = ds.withColumn("First_ECGI_1", substring(ds.col("First_ECGI"), 1, 3))
                .withColumn("First_ECGI_2", substring(ds.col("First_ECGI"), 4, 2))
                .withColumn("First_ECGI_3",(substring(ds.col("First_ECGI"), 6, 5)))
                .withColumn("First_ECGI_4", substring(ds.col("First_ECGI"), 11, 2));


        Column locationCond = ds.col("First_ECGI").isNotNull()
                .and(ds.col("First_ECGI_1").equalTo(col("locationRef.ECGI_1")))
                .and(ds.col("First_ECGI_2").equalTo(col("locationRef.ECGI_2")))
                .and(ds.col("First_ECGI_3").equalTo(col("locationRef.ECGI_3")))
                .and(ds.col("First_ECGI_4").equalTo(col("locationRef.ECGI_4")));

        Column mediaCond0 = ds.col("Call_Identity").equalTo(0)
                .and(
                        ds.col("Destination_IP").equalTo(mediaRef0.col("IP"))
                );

        Column mediaCond1 = ds.col("Call_Identity").equalTo(1)
                .and(
                        ds.col("Source_IP").equalTo(mediaRef1.col("IP1"))
                );
        ds = ds.join(broadcast(locationRef).alias("locationRef") ,locationCond , "leftouter")
                .join(broadcast(mediaRef0).alias("mediaRef0") ,mediaCond0 , "leftouter")
                .join(broadcast(mediaRef1).alias("mediaRef1") ,mediaCond1 , "leftouter")
                .withColumn("SBC_Media" , coalesce(mediaRef0.col("SBC_Media0") , mediaRef1.col("SBC_Media1") , lit("unknown")))
                .drop("First_ECGI_1")
                .drop("First_ECGI_2")
                .drop("First_ECGI_3")
                .drop("First_ECGI_4")
                .drop("ECGI_1")
                .drop("ECGI_2")
                .drop("ECGI_3")
                .drop("ECGI_4")
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
