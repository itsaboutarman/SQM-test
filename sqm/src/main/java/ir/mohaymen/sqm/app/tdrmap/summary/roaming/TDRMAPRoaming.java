package ir.mohaymen.sqm.app.s6a.summary.roaming;

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
public class TDRMAPRoaming extends Summary {

    public TDRMAPRoaming(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }


    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select(
                "Table_ID", "RECORD_DATE", "ROAM_DIRECTION", "HOMEMCC", "HOMEMNC", "VISITCC", "VISITNDC", "PROC_TYPE",
                "SRVTYPE", "SRVSTAT", "CAUSE", "REL_TYPE", "SIG_COLLECTION_TYPE", "PD", "AuthInfoReqCount",
                "AuthInfoRspCount", "AuthInfoDelay", "ISDindCount", "ISDRSPCount", "ISDDELAY",
                "Visited_Country", "Visited_Operator", "Visited_Roaming_Type"
        );
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "15 minutes").getField("start"))
                .withColumnRenamed("VISITCC", "MCC")
                .withColumnRenamed("VISITNDC", "MNC")
                .withColumnRenamed("SRVTYPE", "TRANS_TYPE")
                .withColumnRenamed("SRVSTAT", "TRANS_SUCCED_FLAG")
                .withColumnRenamed("CAUSE", "TRANS_CAUSE");

        ds = ds.withColumn("TRANS_SUCCED_FLAG_ALL", when(col("TRANS_SUCCED_FLAG").isin(0, 1), 1).otherwise(0))
                .withColumn("TRANS_SUCCED_FLAG_TRUE", when(col("TRANS_SUCCED_FLAG").equalTo(1), 1).otherwise(0))
                .withColumn("TRANS_SUCCED_FLAG_FALSE", when(col("TRANS_SUCCED_FLAG").equalTo(0), 1).otherwise(0));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds, new String[]{"Table_ID", "ROAM_DIRECTION", "HOMEMCC", "HOMEMNC",
                "MCC", "MNC", "PROC_TYPE", "TRANS_TYPE", "TRANS_SUCCED_FLAG", "TRANS_CAUSE", "REL_TYPE",
                "SIG_COLLECTION_TYPE", "PD", "AuthInfoReqCount", "AuthInfoRspCount", "AuthInfoDelay",
                "ISDindCount", "ISDRSPCount", "ISDDELAY", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type"});

        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }


    private Dataset<Row> aggregateFunc(Dataset<Row> ds) {
        ds = ds.withWatermark("RECORD_DATE", "1440 minutes")
                // RECORD_DATE is TRANS_REQ_TIME_SEC dim in docs
                .groupBy("Table_ID", "Record_Date", "ROAM_DIRECTION", "HOMEMCC", "HOMEMNC", "MCC", "MNC", "PROC_TYPE",
                        "TRANS_TYPE", "TRANS_SUCCED_FLAG", "TRANS_CAUSE", "REL_TYPE", "SIG_COLLECTION_TYPE", "PD",
                        "AuthInfoReqCount", "AuthInfoRspCount", "AuthInfoDelay", "ISDindCount", "ISDRSPCount",
                        "ISDDELAY", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type")
                .agg(
                        sum("TRANS_SUCCED_FLAG_ALL").alias("Total_Count"),
                        sum("TRANS_SUCCED_FLAG_FALSE").alias("Success_Count"),
                        sum("TRANS_SUCCED_FLAG_TRUE").alias("Fail_Count")
                );
        return ds;
    }
}