package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Map;

@ConnectorAnnotation

public class KafkaSink extends Sink {
    public KafkaSink() {
        super();
    }

    @Override
    public DataStreamWriter<Row> write(Dataset<Row> data){
        data = toJson(data);
        return data.writeStream()
                .format("kafka")
                .options(options)
                .outputMode(options.get("outputMode")).trigger(Trigger.ProcessingTime(options.get("trigger")));
    }

    public Dataset<Row> toJson(Dataset<Row> ds) {
        return ds.select(functions.to_json(functions.struct("*")).alias("value"))
                .selectExpr("CAST(value AS STRING)");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

}
