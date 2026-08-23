package ir.mohaymen.sqm.app.s6a.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;import static org.apache.spark.sql.functions.col;

@Job
public class S6ARoamingFailureCauseLake extends Lake {
    public S6ARoamingFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }
    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("TRANS_SUCCED_FLAG").equalTo(1));
        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }

}
