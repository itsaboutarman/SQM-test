package ir.mohaymen.sqm.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

abstract public class Source extends Connector {
    protected Source(SparkSession session) {
        super();
        this.session = session;
    }

    abstract public Dataset<Row> read();


}
