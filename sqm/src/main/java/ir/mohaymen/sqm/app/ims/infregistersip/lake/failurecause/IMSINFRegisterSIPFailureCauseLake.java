package ir.mohaymen.sqm.app.ims.infregistersip.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.eclipse.jetty.util.ArrayUtil;
import java.util.Map;
import org.apache.spark.sql.Column;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class IMSINFRegisterSIPFailureCauseLake extends Lake {
    public IMSINFRegisterSIPFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
         ds = ds.filter(col("Service_Status").equalTo(1));

        ds = ds.withColumn("RECORD_DATE", from_utc_timestamp(col("RECORD_DATE") ,"Asia/Tehran"))
                .withColumn("day", date_trunc("day", col("RECORD_DATE")))
                .withColumn("hour", hour(col("RECORD_DATE")))
                .withColumn("minuteWindow", lit(minute(col("RECORD_DATE")).minus(minute(col("RECORD_DATE")).mod(5))))
                .withColumn("minute_15", lit(minute(col("RECORD_DATE")).minus(minute(col("RECORD_DATE")).mod(15))));
        
        String[] cols = ds.columns();
        String[] searchCols = {"RECORD_DATE" , "day","hour" ,"minute_15","minuteWindow" ,"Release_Cause_Code",  "Country", "CityName","RegionName", "ProvinceName"};
        for (String col:searchCols){
            cols = ArrayUtil.removeFromArray(cols ,col);
        }

        Column[] columns = {};
        String[] selectedColumns = ArrayUtil.add(searchCols,cols);
        for (String selectedcol:selectedColumns){
            columns = ArrayUtil.addToArray(columns ,col(selectedcol) , selectedcol.getClass());
        }
        ds = ds.select(columns); 
        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }


}
