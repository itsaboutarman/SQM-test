package ir.mohaymen.sqm.app.alarm.baseline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;

public interface BaselineIO extends AutoCloseable {

    /**
     * Reads rows of one table for a time range. Only the given columns are read, and the time filter
     * is sent to the database (push down).
     *
     * @param connection name of the JDBC connection to use (the word after "jdbc." in the config, for example "ims").
     *                   Every table can have its own connection, because the tables can be in different clusters.
     *                   null or empty means the default connection (jdbc.url, jdbc.user ...).
     * @param table   name of the table, for example "ims.summary_inf_register_sip_overview"
     * @param columns comma separated column names, or "*" for all columns
     * @param where   one more condition for the WHERE, in SQL (for example "Country IS NOT NULL"), or null. It is also
     *                sent to the database.
     */
    Dataset<Row> read(String connection, String table, String columns, String where, LocalDateTime from, LocalDateTime to);

    /** Number of rows of the table in the time range (the database counts them). */
    long count(String connection, String table, LocalDateTime from, LocalDateTime to);

    /** The newest RECORD_DATE in the table, or null if the table has no rows. This is where the job resumes. */
    LocalDateTime latestRecordDate(String connection, String table);

    /**
     * Sends the result rows of ONE window to Kafka, one JSON message for each row.
     * Spark's Kafka writer has no transactions: if the job fails in the middle, some messages can already be in Kafka.
     * A message that is sent twice is harmless, because the result table is a Unique Key table.
     *
     * @return the number of messages that were sent
     */
    long write(Dataset<Row> rows);

    @Override
    default void close() {
    }
}