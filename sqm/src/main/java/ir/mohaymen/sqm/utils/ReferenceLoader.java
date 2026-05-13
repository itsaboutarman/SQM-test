package ir.mohaymen.sqm.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class ReferenceLoader {
    SparkSession session;
    Map<String,String> config;
    public ReferenceLoader(SparkSession session ,Map<String,String> config) {
        this.session = session;
        this.config = config;
    }

    public Map<String, Dataset<Row>> loadReferences() {
        Map<String, Dataset<Row>> references = new HashMap<>();
        config.forEach((key, value) -> {
            Dataset<Row> ref = this.session.read()
                    .format(value.substring(value.lastIndexOf(".") + 1))
                    .option("path", value)
                    .load();
            references.put(key, ref);
        });
        return references;
    }
}
