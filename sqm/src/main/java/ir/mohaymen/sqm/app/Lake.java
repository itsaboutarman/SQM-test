package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public abstract class Lake extends SparkApp {

    public Lake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    public Dataset<Row> read(){
        return source.read();
    }

    @Override
    public Dataset<Row> process(Dataset<Row> ds){
        ds = selectColumns(ds);
        ds = manipulateData(ds);
        ds = dropKafkaInfo(ds);
        return ds;
    }

    @Override
    public DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException {
        DataStreamWriter<Row> dw = sink.write(ds);
        dw = partitionData(dw);
        return dw;
    }
    protected abstract Dataset<Row> selectColumns(Dataset<Row> ds);

    private Dataset<Row> dropKafkaInfo(Dataset<Row> ds){
        return ds.drop("partition", "offset");
    }

    protected abstract Dataset<Row> manipulateData(Dataset<Row> ds) ;

    protected abstract DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) ;


}
