package ir.mohaymen.sqm.app.ims.infregistersip.lake.duplication;

import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import ir.mohaymen.sqm.app.Job;
import java.util.Map;
@Job
public class IMSInfRegisterSIPDropDuplicateLake extends Lake {
    public IMSInfRegisterSIPDropDuplicateLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                .dropDuplicates("Session_id", "RECORD_DATE", "Start_Time_ms", "IMEI","Start_Time_s");
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}
