package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

@Job
public abstract class Sourcing extends SparkApp
 {

    public Sourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }
    @Override
    public Dataset<Row> read(){
        return source.read();
    }

    @Override
    public Dataset<Row> process(Dataset<Row> ds){
        ds = extractColumns(ds);
        ds = analyze(ds);
        return ds;
    }
    @Override
    public DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException {
        DataStreamWriter<Row> dw = sink.write(ds);
        ds.printSchema();
        dw = partitionData(dw);
        return dw;
    }
    protected Dataset<Row> addTimeColumns(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE",
                when(col("RECORD_DATE").isNotNull(), col("RECORD_DATE"))
                        .otherwise(to_timestamp(lit("1971-02-03 04:05:06"), "yyyy-MM-dd HH:mm:ss")));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));
        return ds;
    }
    protected abstract Dataset<Row> extractColumns(Dataset<Row> ds);
    protected abstract Dataset<Row> analyze(Dataset<Row> ds);
    protected abstract DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds);
}
