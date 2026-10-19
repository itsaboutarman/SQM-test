package ir.mohaymen.sqm.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;
import java.util.concurrent.atomic.AtomicLong;

public class CustomMetricsSource implements Source {
    private final MetricRegistry metricRegistry;

    // Thread-safe holder for the metric value
    private final AtomicLong avgOffsetsBehindLatest = new AtomicLong(0L);
    AtomicLong removedExecutors = new AtomicLong(0);
    public CustomMetricsSource() {
        this.metricRegistry = new MetricRegistry();
        registerMetrics();
    }
    private void registerMetrics() {
        // Register Gauge — will read latest value whenever JMX polls
        metricRegistry.register(
                MetricRegistry.name("kafka", "averageOffsetsBehindLatestOffset"),
                (Gauge<Long>) avgOffsetsBehindLatest::get
        );

        metricRegistry.register(
                MetricRegistry.name("driver", "removedExecutors"),
                (Gauge<Long>) removedExecutors::get
        );
    }
    // Called from your streaming logic to update the value
    public void updateAverageOffsetsBehind(long value) {
        avgOffsetsBehindLatest.set(value);
    }

    public void incrementFailedExecutors() {
        removedExecutors.incrementAndGet();
    }


    @Override
    public String sourceName() {
        return "kafka_streaming";
    }

    @Override
    public MetricRegistry metricRegistry() {
        return metricRegistry;
    }
}


