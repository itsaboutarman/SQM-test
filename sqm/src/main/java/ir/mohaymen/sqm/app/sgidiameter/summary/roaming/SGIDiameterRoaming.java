package ir.mohaymen.sqm.app.sgidiameter.summary.roaming;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;

import java.util.Map;

@Job
public class SGIDiameterRoaming extends Summary {

    public SGIDiameterRoaming(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Table_ID", "RECORD_DATE", "PROTOCOL_ID", "INTERFACEID", "ROAM_DIRECTION", "HOMEMCC",
                "HOMEMNC", "MCC", "MNC", "TRANS_CAUSE_TYPE", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG"
                , "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                "TRANS_RSP_TIME_SEC", "TRANS_RSP_TIME_MSEC", "TRANS_REQ_TIME_SEC", "TRANS_REQ_TIME_MSEC",
                "USU_CC_TOTAL_OCTETS_1", "USU_CC_TOTAL_OCTETS_2", "USU_CC_TOTAL_OCTETS_3", "RAT",
                "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = ds.withColumn("TRANS_SUCCED_FLAG_ALL", when(col("TRANS_SUCCED_FLAG").isin(0, 1), 1).otherwise(0))
                .withColumn("TRANS_SUCCED_FLAG_TRUE", when(col("TRANS_SUCCED_FLAG").equalTo(1), 1).otherwise(0))
                .withColumn("TRANS_SUCCED_FLAG_FALSE", when(col("TRANS_SUCCED_FLAG").equalTo(0), 1).otherwise(0))
                .withColumn("Delay_Ms", col("TRANS_RSP_TIME_SEC").cast("long").multiply(lit(1000L))
                        .plus(col("TRANS_RSP_TIME_MSEC").cast("long"))
                        .minus(
                                col("TRANS_REQ_TIME_SEC").cast("long").multiply(lit(1000L))
                                        .plus(col("TRANS_REQ_TIME_MSEC").cast("long"))
                        ));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Table_ID",  "PROTOCOL_ID", "INTERFACEID", "ROAM_DIRECTION", "HOMEMCC",
                "HOMEMNC", "MCC", "MNC", "TRANS_CAUSE_TYPE", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG",
                "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type", "RAT", 
                "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3"});

        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                // RECORD_DATE is TRANS_REQ_TIME_SEC dim in docs
                .groupBy("Table_ID", "RECORD_DATE", "PROTOCOL_ID", "INTERFACEID", "ROAM_DIRECTION", "HOMEMCC",
                        "HOMEMNC", "MCC", "MNC", "TRANS_CAUSE_TYPE", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG",
                        "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type", "RAT",
                        "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3")
                .agg(
                        sum("TRANS_SUCCED_FLAG_ALL").alias("Total_Count"),
                        sum("TRANS_SUCCED_FLAG_FALSE").alias("Success_Count"),
                        sum("TRANS_SUCCED_FLAG_TRUE").alias("Fail_Count"),
                        sum("Delay_Ms").alias("Delay_Ms"),
                        sum("USU_CC_TOTAL_OCTETS_1").alias("USU_CC_TOTAL_OCTETS_1"),
                        sum("USU_CC_TOTAL_OCTETS_2").alias("USU_CC_TOTAL_OCTETS_2"),
                        sum("USU_CC_TOTAL_OCTETS_3").alias("USU_CC_TOTAL_OCTETS_3")
                );
        return ds;
    }
}