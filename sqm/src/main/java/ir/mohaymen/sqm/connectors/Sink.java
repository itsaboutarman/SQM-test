package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import java.util.Map;
import java.util.concurrent.TimeoutException;

abstract public class Sink extends Connector{

    protected Sink() {
        super();
    }
    abstract public DataStreamWriter<Row> write(Dataset<Row> data) throws TimeoutException;
}
