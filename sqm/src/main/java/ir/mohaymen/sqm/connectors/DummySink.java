package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@ConnectorAnnotation
public class DummySink extends Sink{

    private static DummySink instance;

    public DummySink() {
        super();
    }

    public static DummySink getInstance(Map<String, String> options) {
        if (instance == null) {
            instance = new DummySink();
        }
        return instance;
    }

    @Override
    public DataStreamWriter<Row> write(Dataset<Row> data) throws TimeoutException {
        return data.writeStream()
                .format("console")
                .options(options)
                .option("truncate", false)
                .outputMode(options.get("outputMode")).trigger(Trigger.ProcessingTime(options.get("trigger")));
    }
}
