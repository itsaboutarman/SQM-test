package ir.mohaymen.sqm.app;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.util.ArrayUtil;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;
import java.util.Map;

@Layer(name = "sampling")
public class SamplingLayer extends AppLayer {
    Map<String,String> config;
    SparkSession session;
    public SamplingLayer(App app , SparkSession session, Map<String,String> config) {
        super(app);
        this.config = config;
        this.session = session;
    }

    @Override
    public Dataset<Row> read() throws Exception {
        Dataset<Row> ds = app.read();
        //apply Sampling:
        ds = filterByHash(ds);
        return ds;
    }

    private Dataset<Row> filterByHash(Dataset<Row> ds) throws Exception {
        String[] hashCols = getHashingColumns();
        int rate = getSamplingRate();
        Column[] cols = new Column[]{};
        for (String colName : hashCols){
            cols = ArrayUtil.addToArray(cols,ds.col(colName), Column.class);
        }
        ds = ds.withColumn("sampling_hash",xxhash64(cols));
        ds = ds.filter(abs(xxhash64(cols)).mod(100).lt(rate));
        return ds;
    }

    private int getSamplingRate() throws Exception {
        int rate = 0;
        try {
            System.out.println( "***sampling rate : " + config.getOrDefault("samplingRate","0"));
            rate = Integer.parseInt(config.getOrDefault("samplingRate","0"));
        } catch (Exception e) {
            throw new RuntimeException("error parsing samplingRate!");
        }

        if (rate > 100 || rate <= 0){
            throw new Exception("samplingRate is must be between 0,100! current Rate=" + rate);
        }
        return rate;
    }
    private String[] getHashingColumns() throws Exception {
        String columnsStr = config.getOrDefault("hashingColumns","");
        if(columnsStr.isEmpty()){
            throw new Exception("hasing columns can not be empty when Sampling enabled.");
        }
        return columnsStr.trim().split(",");
    }

}
