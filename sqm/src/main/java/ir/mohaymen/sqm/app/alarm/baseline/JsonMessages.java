package ir.mohaymen.sqm.app.alarm.baseline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Makes the Kafka message of a row. It is the same conversion as KafkaSink.toJson() of the streaming jobs, so the
 * messages look like the messages of the summary jobs, and StarRocks can read them the same way:
 * one JSON text for each row, in a column called "value". Columns that are NULL are not written.
 */
public final class JsonMessages {

    private JsonMessages() {
    }

    public static Dataset<Row> of(Dataset<Row> rows) {
        return rows.select(functions.to_json(functions.struct("*")).alias("value"))
                .selectExpr("CAST(value AS STRING)");
    }
}
