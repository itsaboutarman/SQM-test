package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

@ConnectorAnnotation
public class StarrocksSink  extends Sink{
    public StarrocksSink() {
        super();
    }
    @Override
    public DataStreamWriter<Row> write(Dataset<Row> data) throws TimeoutException {
        return  data.writeStream().format("starrocks")
                .options(options)
                .outputMode(options.get("outputMode"))
                .trigger(Trigger.ProcessingTime(options.get("trigger")));
    }
}
