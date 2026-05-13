package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import static java.util.stream.Collectors.joining;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public abstract class Summary extends SparkApp {
    public Summary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    public Dataset<Row> read(){
        return source.read();
    }

    @Override
    public Dataset<Row> process(Dataset<Row> ds){
        ds = selectColumns(ds);

        ds = validateRecordTime(ds);

        ds = analyze(ds);
        return ds;
    }

    @Override
    public DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException {
        DataStreamWriter<Row> dw = sink.write(ds);
        return dw;
    }


    protected abstract Dataset<Row> selectColumns(Dataset<Row> ds);

    private Dataset<Row> validateRecordTime(Dataset<Row> ds) {
        return ds.filter(col("RECORD_DATE").leq(current_timestamp().plus(expr("INTERVAL 5 HOURS"))));
    }

    protected abstract Dataset<Row> analyze(Dataset<Row> ds);

    
    protected Dataset<Row> generateKeyHash(Dataset<Row> ds, String[] cols) {
        List<String> columns = Arrays.asList(cols);
        String nvlExpr = columns.stream().map(
                        i -> "nvl(cast ("+i+" as string),'')"
                ).collect(joining(", ", "concat_ws('-'," , ")"));

        return ds.withColumn("key_hash",xxhash64(expr(nvlExpr)));
    }
}
