package ir.mohaymen.sqm.app.s6a.lake.profilinglast;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max_by;
@Job
public class S6AProfilingFinalState extends Lake {

    public S6AProfilingFinalState(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE" ,"60 minutes")
                .groupBy("window_start" , "key_hash" ,"ROAM_DIRECTION","MSISDN",
                "Region", "Visited_Country", "HOMEMCC", "HOMEMNC", "Visited_Operator", "Visited_Roaming_Type")
                .agg(
                        max_by(col("Attachment_Status") , col("RECORD_DATE")).alias("Attachment_Status"),
                        max_by(col("Last_Attachment") , col("RECORD_DATE")).alias("Last_Attachment"),
                        max_by(col("Activation_Status") , col("RECORD_DATE")).alias("Activation_Status"),
                        max_by(col("Affected_Status") , col("RECORD_DATE")).alias("Affected_Status")
                        );
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}
