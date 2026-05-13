package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

@ConnectorAnnotation

public class DeltaSource extends Source {


    public DeltaSource(SparkSession session) {
        super(session);
    }
    public Dataset<Row> read() {
        return session.readStream().format("delta").options(options).load();
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

}
