package ir.mohaymen.sqm.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.joining;

public abstract class AppLayer implements App {
    App app;
    public AppLayer(App app) {
        this.app = app;
    }

    @Override
    public Dataset<Row> read() {
        Dataset<Row> ds = app.read();
        return ds;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> ds) throws Exception {
        ds = app.process(ds);
        return ds;
    }

    @Override
    public DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException {
        return app.write(ds);
    }
}