package ir.mohaymen.sqm.exporters;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import ir.mohaymen.sqm.metrics.CustomMetricsSource;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StatsListener extends StreamingQueryListener {
    private Map<String, String> options;
    private Map<String, Gauge> gaugeMap = new HashMap<>();
    private  CustomMetricsSource customMetricsSource;
    public StatsListener(Map<String, String> options , CustomMetricsSource customMetricsSource) {
        this.options = options;
        this.customMetricsSource = customMetricsSource;
    }

    public void onQueryStarted(final QueryStartedEvent event){
        ;
    }
    public void onQueryProgress(final QueryProgressEvent event) {
        String eventJson = event.progress().prettyJson();
        JSONObject jsonObj = new JSONObject(eventJson);
        try {
            Map<String, Double> data = extractMetrics(jsonObj);
            updateKafkaMetrics(data);
            push(data);
        } catch (Exception e) {
            throw new RuntimeException("Error in extract metric from listener: " , e);
        }
    }
    public void onQueryTerminated(final QueryTerminatedEvent event){
        ;
    }

    private  void updateKafkaMetrics(Map<String, Double> data){
        try {
            double avgAffsetsLag = data.getOrDefault("avgOffsetsBehindLatest",-1d);

            if (avgAffsetsLag != -1d) {
                customMetricsSource.updateAverageOffsetsBehind((long) avgAffsetsLag);
                System.out.println("[KafkaMetrics] avgOffsetsBehindLatest = "
                        + avgAffsetsLag);}
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    private Map<String, Double> extractMetrics(JSONObject jsonObj){
        Map<String, Double> data = new HashMap<>();
        String[] metrics = {"inputRowsPerSecond",
                "processedRowsPerSecond",
                "numInputRows",
                "batchId"};
        for ( String i : metrics) data.put(i, jsonObj.getDouble(i));

        if (options.get("source").toLowerCase().equals("kafka")) {
            JSONObject sourceMetrics = jsonObj.getJSONArray("sources").getJSONObject(0)
                    .getJSONObject("metrics");
            sourceMetrics.keys().forEachRemaining( key -> {
                data.put(key, sourceMetrics.getDouble(key));
                            });
        }
        JSONObject durationMS = jsonObj.getJSONObject("durationMs");
        durationMS.keys().forEachRemaining( key -> {
            data.put(key, durationMS.getDouble(key));
        });
        return data;
    }
    private void push(Map<String, Double> data) throws IOException {
        data.forEach((metricName, metricValue) -> {
            if (gaugeMap.get(metricName) == null) {
                Gauge gauge = Gauge.build()
                        .name(metricName)
                        .help("Spark structured Streaming custom metrics")
                        .register();
                gaugeMap.put(metricName, gauge);
                gauge.set(metricValue);
            }
            else {
                Gauge gauge = gaugeMap.get(metricName);
                gauge.set(metricValue);
            }
        });
        // PushGateway pushGateway = new PushGateway(options.get("prometheus.hostname"));
        // pushGateway.pushAdd(CollectorRegistry.defaultRegistry, options.get("spark.app.name"));
        System.out.println("Stream Batch Metrics");
        System.out.println(data);}
}
