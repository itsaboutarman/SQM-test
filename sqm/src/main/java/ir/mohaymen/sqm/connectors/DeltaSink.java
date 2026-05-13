package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import java.util.Map;
@ConnectorAnnotation
public class DeltaSink extends Sink{
    public DeltaSink() {
        super();
    }
    public DataStreamWriter<Row> write(Dataset<Row> data){
        return data.writeStream()
                .format("delta")
                .options(options)
                .outputMode(options.get("outputMode")).trigger(Trigger.ProcessingTime(options.get("trigger")));
    }
}
