package ir.mohaymen.sqm.app.s6a.lake.profiling;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;

@Job
public class S6aProfilingLake extends Lake {
    public S6aProfilingLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Trans_Type" ,"RECORD_DATE", "ROAM_DIRECTION", "day","MSISDN",
                "TRANS_SUCCED_FLAG","TRANS_CAUSE","TRANS_CAUSE_TYPE", "Region","Visited_Country",
                "HOMEMCC", "HOMEMNC", "Visited_Operator","Visited_Roaming_Type");
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("MSISDN").isNotNull().or(col("MSISDN").notEqual("")));
        ds = ds.withColumn("Attachment_Status",
                        when(col("TRANS_TYPE").equalTo(8).
                                and(col("TRANS_SUCCED_FLAG").equalTo(0)), lit(1))
                                .otherwise(0))
                .withColumn("Last_Attachment" , when(col("Attachment_Status")
                        .equalTo(1) ,col("RECORD_DATE")))
                .withColumn("Activation_Status",
                        when(col("Attachment_Status").equalTo(1), lit(1)).otherwise(0))
                .withColumn("Last_Activation" , when(col("Activation_Status")
                        .equalTo(1) ,col("RECORD_DATE")))
                .withColumn("Affected_Status", when(col("TRANS_TYPE").equalTo(8)
                        .and(col("TRANS_SUCCED_FLAG").equalTo(1))
                        .and(col("TRANS_CAUSE_TYPE").equalTo(1))
                        .and(not(col("TRANS_CAUSE").isin(5004, 5420, 5421))), lit(1)).otherwise(0))
                .withColumn("Last_Affected" ,
                        when(col("Affected_Status").equalTo(1) ,col("RECORD_DATE")));

//        ds = generateKeyHash(ds, new String[]{"MSISDN","ROAM_DIRECTION", "Region", "Visited_Country", "HOMEMCC", "HOMEMNC",
//                "Visited_Operator", "Visited_Roaming_Type"});

        ds = ds.withColumn("window_start", window(col("RECORD_DATE"), "60 minutes").getField("start"));

        ds = ds.withWatermark("RECORD_DATE" ,"60 minutes")
                .groupBy("window_start" , "MSISDN")
                .agg(
                        max_by(col("Attachment_Status") , col("RECORD_DATE")).alias("Attachment_Status"),
                        max_by(col("Last_Attachment") , col("RECORD_DATE")).alias("Last_Attachment"),
                        max_by(col("Activation_Status") , col("RECORD_DATE")).alias("Activation_Status"),
                        max_by(col("Last_Activation") , col("RECORD_DATE")).alias("Last_Activation"),
                        max_by(col("Affected_Status") , col("RECORD_DATE")).alias("Affected_Status"),
                        max_by(col("Last_Affected") , col("RECORD_DATE")).alias("Last_Affected"),
                        max_by(col("ROAM_DIRECTION") , col("RECORD_DATE")).alias("ROAM_DIRECTION"),
                        max_by(col("Region") , col("RECORD_DATE")).alias("Region"),
                        max_by(col("Visited_Country") , col("RECORD_DATE")).alias("Visited_Country"),
                        max_by(col("HOMEMCC") , col("RECORD_DATE")).alias("HOMEMCC"),
                        max_by(col("HOMEMNC") , col("RECORD_DATE")).alias("HOMEMNC"),
                        max_by(col("Visited_Operator") , col("RECORD_DATE")).alias("Visited_Operator"),
                        max_by(col("Visited_Roaming_Type") , col("RECORD_DATE")).alias("Visited_Roaming_Type")
                );
        return ds;

//        Dataset<Row> shifted = ds.withColumn("window_start",expr("window_start + interval 1 hour"))
//                //.withColumn("RECORD_DATE" ,col("window_start"))
//                .withColumn("Activation_Status" , lit(0))
//                .withColumn("Affected_Status" , lit(0));


//        Dataset<Row> result = ds.union(shifted);
//        return result;

//        ds = ds.flatMap((FlatMapFunction<Row, Row>) row -> {
//            List<Row> out = new ArrayList<>();
//            out.add(row);
//            Object[] values = new Object[row.size()];
//            for (int i = 0; i < row.size(); i++) {
//                values[i] = row.get(i);
//            }
//            int tsIndex = row.fieldIndex("window_start");
//            Timestamp windowStart = row.getAs(tsIndex);
//            values[tsIndex] = new Timestamp(windowStart.getTime() + 3600_000);
//            Row nextRow = RowFactory.create(values);
//            out.add(nextRow);
//            return out.iterator();
//        }, ExpressionEncoder.apply(ds.schema()));
//        return ds;
//    }
    }
    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }

    protected Dataset<Row> generateKeyHash(Dataset<Row> ds, String[] cols) {
        List<String> columns = Arrays.asList(cols);
        String nvlExpr = columns.stream().map(
                i -> "nvl(cast ("+i+" as string),'')"
        ).collect(joining(", ", "concat_ws('-'," , ")"));

        return ds.withColumn("key_hash",xxhash64(expr(nvlExpr)));
    }
}
