package ir.mohaymen.sqm.app;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

public interface App {
     Dataset<Row> read() throws Exception;
     Dataset<Row> process(Dataset<Row> ds) throws Exception;
     DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException;

    default StreamingQuery run() throws Exception {
        Dataset<Row> ds = read();
        ds = process(ds);
        DataStreamWriter<Row> dw = write(ds);
        return dw.start();
    }
}