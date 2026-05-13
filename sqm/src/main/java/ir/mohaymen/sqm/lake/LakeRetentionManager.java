package ir.mohaymen.sqm.lake;

import io.delta.tables.DeltaTable;
import ir.mohaymen.sqm.utils.ConfigService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

public class LakeRetentionManager {
    private static final Logger logger = Logger.getLogger(LakeManager.class.getName());
    SparkSession session;
    private final String tablePath;
    private final int THREAD_COUNT = 1;
    private final int START_HOUR = 1;
    private final int START_MINUTE = 0;
    private final int RETENTION_INTERVAL_HOURS = 24;
    private final String DEFAULT_PARTITION = "1970-01-01 00:00:00";
    private int FILE_RETENTION_HOURS = 40;
    private int deletedFileRetentionDuration = 40;
    private int logRetentionDuration = 168;  //7 days
    public LakeRetentionManager(String tablePath, SparkSession session) {
        this.tablePath = tablePath;
        this.session = session;
        setConf();
        logger.info("[Lake Manager] Lake Retention Manager Configs set.");
        start();
    }

    private void setConf() {
        this.FILE_RETENTION_HOURS = Integer.parseInt(ConfigService.getInstance().getOptionsByType("lake").get("FILE_RETENTION_HOURS"));
        this.deletedFileRetentionDuration = Integer.parseInt(ConfigService.getInstance().getOptionsByType("lake").get("DELETED_FILE_RETENTION_HOURS"));
        this.logRetentionDuration = Integer.parseInt(ConfigService.getInstance().getOptionsByType("lake").get("LOG_RETENTION_HOURS"));
    }

    public void start() {
        // setRetentionOnLake();
        scheduleDeleteAndVacuum();
    }

    private void setRetentionOnLake() {
        logger.warning("[Lake Manager] Setting Retention-related TBL Properties...");
        try {
            while (!DeltaTable.isDeltaTable(session, tablePath)) {
                Thread.sleep(5000);
                logger.warning("[Lake Manager] Waiting for the Delta table to be created...");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // set interrupt flag
            logger.severe("[Lake Manager] Failed to wait for the Delta table to be created. The thread was interrupted.");
            return;
        }
        session.sql(String.format(
                "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval %d hours')"
                , tablePath, logRetentionDuration));
        logger.warning("[Lake Manager] delta.logRetentionDuration TBL Property set.");
        session.sql(String.format(
                "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval %d hours')"
                , tablePath, deletedFileRetentionDuration));
        logger.warning("[Lake Manager] delta.deletedFileRetentionDuration TBL Property set.");
    }

    private static class DaemonThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);  // So that the thread exits when the program shuts down
            return thread;
        }
    }

    private void scheduleDeleteAndVacuum() {
        Runnable deleteAndVacuum = defineDeletionAndVacuum();
        logger.warning("[Lake Manager] Scheduling Delete & Vacuum operations: Computing duration until first run.");
        Duration durationUntilStart = getDurationUntilFirstDeleteAndVacuumRun();

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(THREAD_COUNT, new DaemonThreadFactory());

        logger.warning("[Lake Manager] Scheduling Delete & Vacuum operations: Creating Scheduler...");
        scheduler.scheduleAtFixedRate(
                deleteAndVacuum,
                durationUntilStart.getSeconds(),
                Duration.ofHours(RETENTION_INTERVAL_HOURS).getSeconds(),
                TimeUnit.SECONDS
        );
        logger.warning(
                String.format("[Lake Manager] Scheduled Delete & Vacuum operations thread pool to run %d seconds from now, " +
                        "then run at %d seconds interval."
                        , durationUntilStart.getSeconds(), Duration.ofHours(RETENTION_INTERVAL_HOURS).getSeconds()));
    }

    private Duration getDurationUntilFirstDeleteAndVacuumRun() {
        LocalDateTime firstRun = LocalDate.now(ZoneId.of( "Asia/Tehran" )).atStartOfDay()
                .plusHours(START_HOUR)
                .plusMinutes(START_MINUTE);

        Duration durationUntilStart = Duration.between(
                LocalDateTime.now(ZoneId.of( "Asia/Tehran" ))
                , firstRun);
        if(durationUntilStart.isNegative())
            durationUntilStart = Duration.between(
                    LocalDateTime.now(ZoneId.of( "Asia/Tehran" ))
                    , firstRun.plusDays(1));
        return durationUntilStart;
    }

    private Runnable defineDeletionAndVacuum() {
        return () -> {
            try {
                session.sparkContext().setLocalProperty("spark.scheduler.pool", "fair-lakemanager");
                if(!DeltaTable.isDeltaTable(session, tablePath)) {
                    logger.warning(
                            String.format("[Lake Manager] No Delta table was found at %s to perform deletion & vacuum on its data. " +
                                    "Will try again after %d hours.", tablePath, RETENTION_INTERVAL_HOURS));
                    return;
                }
                DeltaTable deltaTable = DeltaTable.forPath(session, tablePath);
                
                LocalDateTime startTime =
                        LocalDateTime.now(ZoneId.of( "Asia/Tehran"));
                logger.warning(
                        String.format("[Lake Manager] Retention Job Started for Delta table %s at %s."
                                , tablePath, startTime.toString()));
                deleteData(deltaTable);
                vacuum(deltaTable);
            }
            catch (Exception e){
                logger.severe(String.format("[Lake Manager] An exception occurred while deleting or vacuuming lake: " + e.getMessage()));
            }
        };
    }

    private void vacuum(DeltaTable deltaTable) {
        //This method will return an empty DataFrame on successful completion.
        Dataset<Row> result = deltaTable.vacuum(deletedFileRetentionDuration);
        if(!result.isEmpty()) {
            logger.severe(
                    String.format("[Lake Manager] Vacuuming Delta table %s with retention duration %d hours failed."
                            , tablePath, deletedFileRetentionDuration));
        }
        else{
            logger.warning(
                    String.format("[Lake Manager] Successfully vacuumed Delta table %s with retention duration %d hours."
                            , tablePath, deletedFileRetentionDuration));
        }
    }

    private void deleteData(DeltaTable deltaTable) {
        LocalDateTime lastDateTimeToRetain =
                LocalDate.now(ZoneId.of( "Asia/Tehran" )).atStartOfDay()
                        .plusHours(START_HOUR).plusMinutes(START_MINUTE)
                        .minusHours(FILE_RETENTION_HOURS);

        deltaTable.delete((col("day").lt(lastDateTimeToRetain))
                .and(col("day").notEqual(DEFAULT_PARTITION)));
        logger.warning(
                String.format("[Lake Manager] Successfully deleted Delta table %s data older than %s."
                        , tablePath, lastDateTimeToRetain.toString()));
    }
}
