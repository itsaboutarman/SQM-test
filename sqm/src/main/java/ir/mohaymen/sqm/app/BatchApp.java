package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.utils.ConfigService;
import org.apache.spark.sql.SparkSession;

public abstract class BatchApp {
    protected SparkSession session;
    protected ConfigService config;
    public BatchApp(SparkSession session , ConfigService config){
        this.session = session;
        this.config = config;
    }
    public abstract BatchAppResult run() throws Exception;
}


