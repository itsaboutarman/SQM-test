package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@ConnectorAnnotation

public class KafkaSource extends Source{
    public KafkaSource(SparkSession session) {
        super(session);
    }

    @Override
    public Dataset<Row> read() {
        return session.readStream()
                .format("kafka")
                .options(options)
                .load()
                .selectExpr("CAST(value AS STRING)", "partition", "offset", "timestamp");
    }
}
