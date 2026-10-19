package ir.mohaymen.sqm.app.alarm.baseline;

import ir.mohaymen.sqm.app.BatchApp;
import ir.mohaymen.sqm.app.BatchAppResult;
import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.utils.ConfigService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

@Job
public class AlarmBaseline extends BatchApp {

    private static final Logger logger = Logger.getLogger(AlarmBaseline.class.getName());

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final long POLL_MILLIS = 15000;

    private final BaselineIO io;
    private final Supplier<LocalDateTime> clock;
    private final LongConsumer sleeper;
    private final List<LocalDateTime> processedWindows = new ArrayList<LocalDateTime>();

    /** Used by the framework (AppBuilder.buildBatchApp). */
    public AlarmBaseline(SparkSession session, ConfigService config) {
        this(session, config, new JdbcKafkaBaselineIO(session, config.getOptions()), new Supplier<LocalDateTime>() {
            @Override
            public LocalDateTime get() {
                return LocalDateTime.now(ZoneOffset.UTC);
            }
        }, new LongConsumer() {
            @Override
            public void accept(long millis) {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting", e);
                }
            }
        });
    }

    public AlarmBaseline(SparkSession session, ConfigService config, BaselineIO io, Supplier<LocalDateTime> clock, LongConsumer sleeper) {
        super(session, config);
        this.io = io;
        this.clock = clock;
        this.sleeper = sleeper;
    }

    /** The windows (their start) that this object has calculated, in the order of the work. */
    public List<LocalDateTime> getProcessedWindows() {
        return Collections.unmodifiableList(processedWindows);
    }

    @Override
    public BatchAppResult run() throws Exception {
        try {
            catchUp();
        } finally {
            io.close();
        }
        return new BatchAppResult();
    }

    private void catchUp() {
        Map<String, String> options = config.getOptions();
        Settings s = new Settings(options);

        LocalDateTime target = latestDueWindow(clock.get(), s.windowMinutes, s.delayMinutes);
        LocalDateTime next = firstWindow(options, s, target);

        if (next.isBefore(target.minusHours(s.maxBacklogHours))) {
            throw new IllegalStateException("The job is more than " + s.maxBacklogHours + " hours behind: the next window is " + next
                    + " and the newest window that is due is " + target + ". A person must decide how to continue."
                    + " To go on from the table position, increase baseline.maxBacklogHours.");
        }
        long due = next.isAfter(target) ? 0 : Duration.between(next, target).toMinutes() / s.windowMinutes + 1;
        logger.info(String.format("[AlarmBaseline] first window %s, newest window that is due %s (%d windows)", next, target, due));

        Map<LocalDateTime, Long> sent = new HashMap<LocalDateTime, Long>();   // window -> messages sent in this run
        while (true) {
            // New windows become due while the job runs: ask the clock again every time
            target = latestDueWindow(clock.get(), s.windowMinutes, s.delayMinutes);
            if (next.isAfter(target)) {
                break;
            }
            long messages = processWindow(next, s, sent);
            sent.put(next, messages);
            processedWindows.add(next);
            next = next.plusMinutes(s.windowMinutes);

            // only the windows of the last day can be needed as "one day earlier"
            LocalDateTime oldest = next.minusDays(1).minusMinutes(s.windowMinutes);
            for (Iterator<LocalDateTime> it = sent.keySet().iterator(); it.hasNext(); ) {
                if (it.next().isBefore(oldest)) {
                    it.remove();
                }
            }
        }
        logger.info("[AlarmBaseline] the job is on time. The next window is " + next);
    }

    /** One window: read, calculate, send to Kafka. Returns the number of messages. */
    private long processWindow(LocalDateTime from, Settings s, Map<LocalDateTime, Long> sent) {
        LocalDateTime to = from.plusMinutes(s.windowMinutes);
        LocalDateTime previousFrom = from.minusDays(1);
        LocalDateTime previousTo = to.minusDays(1);
        long t0 = System.currentTimeMillis();

        // 1) the rows of the window: StarRocks only gives the rows and the columns that are needed
        String where = AlarmBaselineKpis.COUNTRY_FILTER_SQL;
        Dataset<Row> register = io.read(s.registerConnection, s.registerTable, AlarmBaselineKpis.REGISTER_COLUMNS, where, from, to);
        Dataset<Row> call = io.read(s.callConnection, s.callTable, AlarmBaselineKpis.CALL_COLUMNS, where, from, to);
        Dataset<Row> tcp = io.read(s.tcpConnection, s.tcpTable, AlarmBaselineKpis.TCP_COLUMNS, where, from, to);

        // 2) the result rows of the same window one day earlier
        Long expected = sent.get(previousFrom);
        if (expected != null && expected > 0) {
            // This run sent that window itself (a long outage). Wait until StarRocks has loaded it.
            waitUntilLoaded(s, previousFrom, previousTo, expected);
        }
        long previousRows = io.count(s.targetConnection, s.targetTable, previousFrom, previousTo);
        if (previousRows == 0) {
            String message = "There are no rows in " + s.targetTable + " for the window " + previousFrom + " (one day before "
                    + from + "). All num_d* and denom_d* of this window will be NULL. If this is not the first day, that window"
                    + " has no data, or it is not loaded (is the Routine Load running?).";
            if (s.failIfNoPreviousDay) {
                throw new IllegalStateException(message);
            }
            logger.warning("[AlarmBaseline] " + message);
        }
        Dataset<Row> previous = io.read(s.targetConnection, s.targetTable, AlarmBaselineKpis.BASELINE_COLUMNS, null, previousFrom, previousTo);

        // 3) the calculation in Spark, and the result to Kafka
        Dataset<Row> withHistory = AlarmBaselineKpis.withBaseline(AlarmBaselineKpis.allKpis(register, call, tcp), previous);
        Dataset<Row> result = AlarmBaselineKpis.withKeyHash(withHistory);       // the primary key of the table: (RECORD_DATE, key_hash)
        long messages = io.write(result);
        logger.info(String.format("[AlarmBaseline] window [%s, %s): %d messages sent in %d s", from, to, messages,
                (System.currentTimeMillis() - t0) / 1000));
        return messages;
    }

    /** Waits until the rows of a window that was sent in this run are in the result table. */
    private void waitUntilLoaded(Settings s, LocalDateTime from, LocalDateTime to, long expectedRows) {
        long waited = 0;
        long limit = s.waitForLoadMinutes * 60000L;
        while (true) {
            if (io.count(s.targetConnection, s.targetTable, from, to) >= expectedRows) {
                return;
            }
            if (waited >= limit) {
                throw new IllegalStateException("The window " + from + " was sent (" + expectedRows + " messages), but it is not in "
                        + s.targetTable + " after " + s.waitForLoadMinutes + " minutes. The next window needs it. Is the Routine Load running?");
            }
            sleeper.accept(POLL_MILLIS);
            waited += POLL_MILLIS;
        }
    }

    // ------------------------------------------------------------------------------------------------
    // windows
    // ------------------------------------------------------------------------------------------------

    /**
     * Start of the newest window that is due at the time "now": now is rounded down to a window, and then the
     * delay and one window are taken away. (A run at 01:00 with 15 minute windows and 45 minutes delay: 00:00.)
     */
    static LocalDateTime latestDueWindow(LocalDateTime now, int windowMinutes, int delayMinutes) {
        return floor(now, windowMinutes).minusMinutes(delayMinutes).minusMinutes(windowMinutes);
    }

    /** Rounds down to the start of the window. Windows start at 00:00 of every day and follow each other. */
    static LocalDateTime floor(LocalDateTime time, int windowMinutes) {
        int minuteOfDay = time.getHour() * 60 + time.getMinute();
        return time.withSecond(0).withNano(0).minusMinutes(minuteOfDay % windowMinutes);
    }

    /**
     * Where to start. The result table is asked FIRST, in every run:
     *   1. the window after the newest RECORD_DATE in the result table (the last checkpoint),
     *   2. if the table is empty: baseline.resumeFrom, if it is set (the first run),
     *   3. otherwise: only the newest window that is due.
     * So baseline.resumeFrom can stay in the config. It is only used while the table is empty.
     */
    private LocalDateTime firstWindow(Map<String, String> options, Settings s, LocalDateTime target) {
        LocalDateTime latest = io.latestRecordDate(s.targetConnection, s.targetTable);
        if (latest != null) {
            return floor(latest, s.windowMinutes).plusMinutes(s.windowMinutes);
        }

        String resumeFrom = options.get("baseline.resumeFrom");
        if (resumeFrom != null && !resumeFrom.trim().isEmpty()) {
            LocalDateTime start = LocalDateTime.parse(resumeFrom.trim(), TIME);
            if (!start.equals(floor(start, s.windowMinutes))) {
                throw new IllegalArgumentException("baseline.resumeFrom is not the start of a " + s.windowMinutes + " minute window: " + resumeFrom);
            }
            logger.info("[AlarmBaseline] " + s.targetTable + " has no rows: starting with baseline.resumeFrom = " + start);
            return start;
        }
        logger.info("[AlarmBaseline] " + s.targetTable + " has no rows and baseline.resumeFrom is not set: starting with the newest window that is due");
        return target;
    }

    // ------------------------------------------------------------------------------------------------
    // config
    // ------------------------------------------------------------------------------------------------

    /** The options of the job (see config.properties). */
    private static final class Settings {
        final int windowMinutes;
        final int delayMinutes;
        final long maxBacklogHours;
        final int waitForLoadMinutes;
        final boolean failIfNoPreviousDay;
        final String registerTable;
        final String callTable;
        final String tcpTable;
        final String targetTable;
        // The name of the JDBC connection of each table (jdbc.<name>.url ...). Empty = the default connection (jdbc.url).
        final String registerConnection;
        final String callConnection;
        final String tcpConnection;
        final String targetConnection;

        Settings(Map<String, String> o) {
            windowMinutes = Integer.parseInt(o.getOrDefault("baseline.windowMinutes", "15").trim());
            delayMinutes = Integer.parseInt(o.getOrDefault("baseline.delayMinutes", "45").trim());
            maxBacklogHours = Long.parseLong(o.getOrDefault("baseline.maxBacklogHours", "168").trim());
            waitForLoadMinutes = Integer.parseInt(o.getOrDefault("baseline.waitForLoadMinutes", "10").trim());
            failIfNoPreviousDay = Boolean.parseBoolean(o.getOrDefault("baseline.failIfNoPreviousDay", "false").trim());
            registerTable = require(o, "baseline.source.register.table");
            callTable = require(o, "baseline.source.call.table");
            tcpTable = require(o, "baseline.source.tcp.table");
            targetTable = require(o, "baseline.target.table");
            registerConnection = o.getOrDefault("baseline.source.register.connection", "").trim();
            callConnection = o.getOrDefault("baseline.source.call.connection", "").trim();
            tcpConnection = o.getOrDefault("baseline.source.tcp.connection", "").trim();
            targetConnection = o.getOrDefault("baseline.target.connection", "").trim();
            if (windowMinutes < 1 || 1440 % windowMinutes != 0) {
                throw new IllegalArgumentException("baseline.windowMinutes must divide a day (1440 minutes) without rest: " + windowMinutes);
            }
            if (delayMinutes < 0 || delayMinutes % windowMinutes != 0) {
                // otherwise the windows of a run would not be the windows of the grid (00:00, 00:15, ...)
                throw new IllegalArgumentException("baseline.delayMinutes must be a whole number of windows (a multiple of " + windowMinutes + "): " + delayMinutes);
            }
        }
    }

    private static String require(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Config key is missing: " + key);
        }
        return value.trim();
    }
}