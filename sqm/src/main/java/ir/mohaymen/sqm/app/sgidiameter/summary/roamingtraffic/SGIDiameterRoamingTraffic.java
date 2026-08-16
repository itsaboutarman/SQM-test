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
public class SGIDiameterRoamingTraffic extends Summary {

    public SGIDiameterRoamingTraffic(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("Table_ID", "TRANS_RSP_TIME_SEC", "PROTOCOL_ID", "RAT", "INTERFACEID", "ROAM_DIRECTION",
                "HOMEMCC", "HOMEMNC", "MCC", "MNC", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG",
                "TRANS_CAUSE_TYPE", "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3",
                "USU_CC_TOTAL_OCTETS_1", "USU_CC_TOTAL_OCTETS_2", "USU_CC_TOTAL_OCTETS_3"
        );
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("TRANS_RSP_TIME_SEC")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));

        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Table_ID", "PROTOCOL_ID", "RAT", "INTERFACEID", "ROAM_DIRECTION",
                "HOMEMCC", "HOMEMNC", "MCC", "MNC", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG",
                "TRANS_CAUSE_TYPE", "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3"});

        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                // RECORD_DATE is TRANS_REQ_TIME_SEC dim in docs
                .groupBy("Table_ID", "RECORD_DATE", "PROTOCOL_ID", "RAT", "INTERFACEID", "ROAM_DIRECTION",
                        "HOMEMCC", "HOMEMNC", "MCC", "MNC", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG",
                        "TRANS_CAUSE_TYPE", "TRANS_CAUSE", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                        "USU_RATING_GROUP_1", "USU_RATING_GROUP_2", "USU_RATING_GROUP_3")
                .agg(
                        sum("USU_CC_TOTAL_OCTETS_1").alias("USU_CC_TOTAL_OCTETS_1"),
                        sum("USU_CC_TOTAL_OCTETS_2").alias("USU_CC_TOTAL_OCTETS_2"),
                        sum("USU_CC_TOTAL_OCTETS_3").alias("USU_CC_TOTAL_OCTETS_3")
                );
        return ds;
    }
}