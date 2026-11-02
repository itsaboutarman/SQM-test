package ir.mohaymen.sqm.app.batch.alarm.baseline;

import ir.mohaymen.sqm.app.batch.alarm.baseline.BaselineIO;
import ir.mohaymen.sqm.app.batch.alarm.baseline.JsonMessages;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class JdbcKafkaBaselineIO implements BaselineIO {

    private static final DateTimeFormatter SQL_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final SparkSession session;
    /** connection name -> its Spark JDBC options. The default connection (jdbc.url ...) has the name "". */
    private final Map<String, Map<String, String>> connections = new HashMap<String, Map<String, String>>();
    private final Map<String, String> kafkaOptions = new HashMap<String, String>();

    public JdbcKafkaBaselineIO(SparkSession session, Map<String, String> config) {
        this.session = session;
        for (Map.Entry<String, String> e : config.entrySet()) {
            String key = e.getKey();
            if (key.startsWith("jdbc.")) {
                String rest = key.substring("jdbc.".length());
                int dot = rest.indexOf('.');
                String name = dot < 0 ? "" : rest.substring(0, dot);
                String option = dot < 0 ? rest : rest.substring(dot + 1);
                Map<String, String> options = connections.get(name);
                if (options == null) {
                    options = new HashMap<String, String>();
                    connections.put(name, options);
                }
                options.put(option, e.getValue());
            } else if (key.startsWith("sink.")) {
                String name = key.substring("sink.".length());
                if (name.equals("topic") || name.startsWith("kafka.")) {
                    kafkaOptions.put(name, e.getValue());
                }
            }
        }
        if (connections.isEmpty()) {
            throw new IllegalArgumentException("Config key is missing: jdbc.<name>.url (one for each connection, for example jdbc.ims.url)");
        }
        for (Map.Entry<String, Map<String, String>> c : connections.entrySet()) {
            String url = c.getValue().get("url");
            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("Config key is missing: " + (c.getKey().isEmpty() ? "jdbc.url" : "jdbc." + c.getKey() + ".url"));
            }
        }
        require(kafkaOptions, "topic", "sink.topic");
        require(kafkaOptions, "kafka.bootstrap.servers", "sink.kafka.bootstrap.servers");
    }

    private static void require(Map<String, String> options, String name, String configKey) {
        if (options.get(name) == null || options.get(name).trim().isEmpty()) {
            throw new IllegalArgumentException("Config key is missing: " + configKey);
        }
    }

    /** The JDBC options of one connection (a copy). null or empty means the default connection. */
    private Map<String, String> connection(String name) {
        String key = name == null ? "" : name.trim();
        Map<String, String> options = connections.get(key);
        if (options == null) {
            throw new IllegalArgumentException("The JDBC connection '" + key + "' is not in the config. Add " + (key.isEmpty() ? "jdbc.url" : "jdbc." + key + ".url")
                    + " (and user, password, driver). It is used by a table: check baseline.source.*.connection and baseline.target.connection.");
        }
        return new HashMap<String, String>(options);
    }

    @Override
    public Dataset<Row> read(String connection, String table, String columns, String where, LocalDateTime from, LocalDateTime to) {
        // The WHERE is executed inside StarRocks, so only the rows of the time range travel to Spark.
        String condition = timeFilter(from, to) + (where == null || where.trim().isEmpty() ? "" : " AND (" + where.trim() + ")");
        String query = "(SELECT " + columns + " FROM " + table + " WHERE " + condition + ") AS t";

        Map<String, String> options = connection(connection);
        DataFrameReader reader = session.read().format("jdbc").options(options).option("dbtable", query);

        int parts = numPartitions(options);
        if (parts > 1) {
            // Spark makes one query for each part of the time range and runs them at the same time.
            reader = reader.option("partitionColumn", "RECORD_DATE")
                    .option("lowerBound", SQL_TIME.format(from))
                    .option("upperBound", SQL_TIME.format(to))
                    .option("numPartitions", String.valueOf(parts));
        }
        return reader.load();
    }

    @Override
    public long count(String connection, String table, LocalDateTime from, LocalDateTime to) {
        String query = "(SELECT COUNT(*) AS c FROM " + table + " WHERE " + timeFilter(from, to) + ") AS t";
        return ((Number) singleValue(connection, query)).longValue();
    }

    @Override
    public LocalDateTime latestRecordDate(String connection, String table) {
        Object value = singleValue(connection, "(SELECT MAX(RECORD_DATE) AS m FROM " + table + ") AS t");
        return value == null ? null : ((Timestamp) value).toLocalDateTime();
    }

    /** One small query that returns one value (one row, one column). */
    private Object singleValue(String connection, String query) {
        Map<String, String> options = connection(connection);
        options.remove("numPartitions");          // one small query: no parts
        Row row = session.read().format("jdbc").options(options).option("dbtable", query).load().first();
        return row.isNullAt(0) ? null : row.get(0);
    }

    @Override
    public long write(Dataset<Row> rows) {
        // Keep the result of the window, so it is calculated only once, and the number of messages is the same
        // as the number of messages that are sent.
        Dataset<Row> json = JsonMessages.of(rows).persist(StorageLevel.MEMORY_AND_DISK());
        try {
            long count = json.count();
            // The executors write to Kafka directly (Spark's own Kafka writer). The column "value" is the message.
            json.write().format("kafka").options(kafkaOptions).save();
            return count;
        } finally {
            json.unpersist();
        }
    }

    private static int numPartitions(Map<String, String> options) {
        String value = options.get("numPartitions");
        return value == null ? 1 : Integer.parseInt(value.trim());
    }

    private static String timeFilter(LocalDateTime from, LocalDateTime to) {
        return "RECORD_DATE >= '" + SQL_TIME.format(from) + "' AND RECORD_DATE < '" + SQL_TIME.format(to) + "'";
    }
}