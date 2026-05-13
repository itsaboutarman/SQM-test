package ir.mohaymen.sqm.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// TODO: better to handle different types of SparkSessions and FlinkContexts and ...
public class SparkSessionHandler {
    private static SparkSession session;
    public static SparkConf conf = new SparkConf();
//    private String type = "spark";

    protected Map<String, String> options = new HashMap<>();


    private SparkSessionHandler() {
    }

    public static SparkSession getSession() {
        setSparkOptions(ConfigService.getInstance().getOptions());
        session = SparkSession.builder().master("local").config(conf).getOrCreate();
        session.sparkContext().setLogLevel(ConfigService.getInstance().getOptions().get("logLevel"));
        return session;

    }

    public static SparkSession getInstanceSingleton() {
        if (session == null) {
            setSparkOptions(ConfigService.getInstance().getOptions());
            session = SparkSession.builder().master("local").config(conf).getOrCreate();
            session.sparkContext().setLogLevel(ConfigService.getInstance().getOptions().get("logLevel"));
        }
        return session;
    }

    private static void setSparkOptions(Map<String, String> options) {
        options.forEach((key, value) -> {
            if (key.startsWith("spark"))
                conf.set(key, value);

        });

    }

    public static void killInstance() throws IOException {
        session.close();
    }

}
