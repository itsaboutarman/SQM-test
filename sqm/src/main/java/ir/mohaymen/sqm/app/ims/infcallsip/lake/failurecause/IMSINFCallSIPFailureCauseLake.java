package ir.mohaymen.sqm.app.ims.infcallsip.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.minute;

@Job
public class IMSINFCallSIPFailureCauseLake extends Lake {
    public IMSINFCallSIPFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
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
        ds = ds.withWatermark("RECORD_DATE", "24 hours");
        ds.printSchema();

        String[] cols = ds.columns();
        String[] searchCols = {"RECORD_DATE" , "day","hour" ,"minuteWindow" ,"Release_Cause_Code","Service_Type"
                ,"Interface_Type","Calling_or_Called_Party_Indicator" , "Source_NE_Type","Destination_NE_Type" , "Country", "CityName","RegionName", "ProvinceName"};
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