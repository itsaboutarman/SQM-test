package ir.mohaymen.sqm.exporters;

import ir.mohaymen.sqm.app.App;
import ir.mohaymen.sqm.app.Lake;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.Map;
import java.util.logging.Logger;

public class KeepThriftUpdatedListener extends StreamingQueryListener {
    private final String deltaPath;
    private final SparkSession session;

    public static boolean shouldKeepThriftUpdated(App app, Map<String, String> options) {
        return app instanceof Lake && (options.get("sink").equalsIgnoreCase("delta"));
    }

    public KeepThriftUpdatedListener(SparkSession session, Map<String, String> options) {
        this.deltaPath = options.get("sink.path");
        this.session = session;
    }

    public void onQueryStarted(final QueryStartedEvent event){
        ;
    }
    public void onQueryProgress(final QueryProgressEvent event) {
        try {
            sendQuery();
        } catch (Exception e) {
            throw new RuntimeException("Error when running KeepThriftUpdatedListener: " , e);
        }
    }
    public void onQueryTerminated(final QueryTerminatedEvent event){
        ;
    }

    public void sendQuery() {
        System.out.println("[Keep Thrift Updated] Trying to send query to thrift: " 
                    + "select * from delta.`" + this.deltaPath + "` limit 2");
        this.session.read().format("jdbc").option("url", System.getenv("THRIFT_ADDRESS"))
                .option("username", System.getenv("THRIFT_USER")).option("password", System.getenv("THRIFT_PASSWD"))
                .option("query", "select * from delta.`" + this.deltaPath + "` limit 2").load();
        System.out.println("[Keep Thrift Updated] Query executed by thrift.");
    }
}
