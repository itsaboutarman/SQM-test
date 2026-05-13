package ir.mohaymen.sqm.app.streaming.lake.disconnectiontimewindow;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class StreamingDisconnectionTimeWindowLake extends Lake {
    public StreamingDisconnectionTimeWindowLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")))
                .withColumn("hour", hour(col("RECORD_DATE")))
                .withColumn("minuteWindow",
                        lit(minute(col("RECORD_DATE")).minus(minute(col("RECORD_DATE")).mod(5))));

        ds = ds.filter(col("STREAMING_DISCONNECT_FLAG").equalTo(0));
        // return ds;
        return ds.repartition(10, col("day"), col("hour"), col("minuteWindow"));
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day", "hour", "minuteWindow");
    }


}
