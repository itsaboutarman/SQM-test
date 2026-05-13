package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

abstract public class SparkApp implements App {
    protected Source source;
    protected Sink sink;
    protected Map<String, Dataset<Row>> references;
    public StreamingQuery streamingQuery;
    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }
    public void setReferences(Map<String, Dataset<Row>> references){this.references = references;}

    public Sink getSink() {
        return sink;
    }

    public void setSink(Sink sink) {
        this.sink = sink;
    }


    public SparkApp(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        setSource(source);
        setSink(sink);
        setReferences(references);
    }

    public abstract Dataset<Row> read();
    public abstract Dataset<Row> process(Dataset<Row> ds);
    public abstract DataStreamWriter<Row> write(Dataset<Row> ds) throws TimeoutException;


    protected Dataset<Row> getReference(String referenceName) {
        if (references.containsKey(referenceName))
            return references.get(referenceName);
        else throw new NullPointerException(
                String.format("Reference %s does not exist among provided references.", referenceName)
        );
    }
}
