package ir.mohaymen.sqm.app.dns.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class DNSFailureCauseLake extends Lake {
    public DNSFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds;
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.filter(col("day").geq(lit("2025-02-07 00:00:00")));
        ds = ds.withColumn("RECORD_DATE", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")))
                .withColumn("day", date_trunc("day", col("RECORD_DATE")))
                .withColumn("hour", hour(col("RECORD_DATE")))
                .withColumn("minuteWindow",
                        lit(minute(col("RECORD_DATE")).minus(minute(col("RECORD_DATE")).mod(5))));

        ds = ds.withColumnRenamed("ERROR_CODE_FST", "First_Request_Failure_Cause_Value")
                .withColumnRenamed("FST_TRANS_FLAG", "First_Transaction_Success_or_Failure_Flag");

        ds = ds.filter(col("First_Transaction_Success_or_Failure_Flag").equalTo(1)
                    .and(col("Country").isNotNull()).and(col("Country").notEqual(lit("")))
                .and(col("First_Request_Failure_Cause_Value").isNotNull()));

       return ds.repartition(10, col("day"), col("hour"), col("minuteWindow"),
               col("First_Request_Failure_Cause_Value"));
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day", "hour", "minuteWindow", "First_Request_Failure_Cause_Value");
    }


}
