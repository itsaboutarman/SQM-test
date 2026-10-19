package ir.mohaymen.sqm.exporters;

import ir.mohaymen.sqm.metrics.CustomMetricsSource;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

import java.util.concurrent.atomic.AtomicLong;

public class AppStatusListener extends SparkListener {
    CustomMetricsSource metricSource;
    public AppStatusListener(CustomMetricsSource source){
        this.metricSource = source;
    }
    @Override
    public void onExecutorRemoved(final SparkListenerExecutorRemoved executorRemoved) {
        metricSource.incrementFailedExecutors();
        System.out.println("!!executor removed");
    }
}
