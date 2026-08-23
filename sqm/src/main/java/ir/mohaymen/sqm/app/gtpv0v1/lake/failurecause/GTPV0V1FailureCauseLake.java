package ir.mohaymen.sqm.app.gtpv0v1.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.minute;

@Job
public class GTPV0V1FailureCauseLake extends Lake {
    public GTPV0V1FailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("INTERFACEID").isin(1, 13)
                .and(col("TRANS_SUCCED_FLAG").equalTo(1)))
                .drop("offset")
                .drop("partition")
                .drop("timestamp")
                .drop("day");
        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}