package ir.mohaymen.sqm.exporters;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BatchIdListener extends StreamingQueryListener {

    private final AtomicLong batchId;

    public BatchIdListener(AtomicLong batchId) {
        this.batchId = batchId;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
        // No action needed on query start
    }

    @Override
    public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
        batchId.incrementAndGet();
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
        // No action needed on query termination
    }
}