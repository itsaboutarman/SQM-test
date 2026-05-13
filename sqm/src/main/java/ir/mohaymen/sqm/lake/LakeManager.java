package ir.mohaymen.sqm.lake;

import io.delta.tables.DeltaTable;
import ir.mohaymen.sqm.utils.ConfigService;
import ir.mohaymen.sqm.utils.SparkSessionHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.*;
import java.util.logging.Logger;
import static org.apache.spark.sql.functions.*;

public class LakeManager {
    private static final Logger logger = Logger.getLogger(LakeManager.class.getName());
    SparkSession session = SparkSessionHandler.getInstanceSingleton();
    private static LakeManager instance;
    private String tablePath;
    public LakeRetentionManager retentionManager;
    private LakeManager() {
        this.tablePath = ConfigService.getInstance().getOptionsByType("sink").get("path");
    }

    public static LakeManager initialize(){
        if (instance == null ) {
            logger.warning("[Lake Manager] Initializing LakeManager...");
            System.out.println("[Lake Manager without logger] Initializing LakeManager...");
            instance = new LakeManager();
            if (needsRetention()) {
                logger.warning("[Lake Manager] Initializing Lake Retention Manager...");
                instance.retentionManager = new LakeRetentionManager(instance.tablePath, instance.session);
            } else {
                logger.severe("[Lake Manager] No Retention Policy Needed.");
            }
        }
        return instance;
    }

    private static boolean needsRetention() {
        return ConfigService.getInstance().getOptionsByType("lake").get("retention").equalsIgnoreCase("true");
    }

    private Boolean optimizeLake() {
        return true;
    }


}
