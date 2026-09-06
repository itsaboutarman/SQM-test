package ir.mohaymen.sqm.app.tdrmap.lake.failurecause;

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
public class TDRMAPFailureCauseLake extends Lake {
    public TDRMAPFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {

        return ds.select("RECORD_DATE", "SRVSTAT", "ROAM_DIRECTION", "TABLE_ID", "TDRID", "STARTTIME", "MILLISEC", "NI",
                "OPC", "OSSN", "OGT", "DPC", "DSSN", "DGT", "SRVTYPE", "HLRNO", "SGSNNO", "SGSNIP", "MSISDN", "IMSI",
                "TMSI", "IMEI", "HOMEMCC", "HOMEMNC", "VISITCC", "VISITNDC", "SRVRSPTIME", "END_TIME", "REL_TYPE",
                "CAUSE", "SIG_COLLECTION_TYPE", "PROC_TYPE", "PD", "AuthInfoReqCount", "AuthInfoRspCount",
                "AuthInfoDelay", "ISDindCount", "ISDRSPCount", "ISDDELAY", "col33", "col34", "col35",
                "RECORD_DATE_TEHRAN", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type");

    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("SRVSTAT").equalTo(1))
                .drop("offset")
                .drop("partition")
                .drop("timestamp")
                .drop("day");

        ds = ds.withColumnRenamed("SRVSTAT", "SRVSTAT_source")
                .withColumnRenamed("TABLE_ID", "TABLE_ID_source")
                .withColumnRenamed("NI", "NI_source")
                .withColumnRenamed("SRVTYPE", "SRVTYPE_source")
                .withColumnRenamed("SIG_COLLECTION_TYPE", "SIG_COLLECTION_TYPE_source")
                .withColumnRenamed("PD", "PD_source")
                .withColumnRenamed("AuthInfoReqCount", "AuthInfoReqCount_source")
                .withColumnRenamed("AuthInfoRspCount", "AuthInfoRspCount_source")
                .withColumnRenamed("ISDindCount", "ISDindCount_source")
                .withColumnRenamed("ISDRSPCount", "ISDRSPCount_source")

                .withColumn("SRVSTAT", when(col("SRVSTAT_source").isNull(), lit(null))
                        .when(col("SRVSTAT_source").equalTo(0), "Successful")
                        .when(col("SRVSTAT_source").equalTo(1), "Failed")
                        .otherwise(lit("unknown")))

                .withColumn("ROAM_DIRECTION_Label", when(col("ROAM_DIRECTION").isNull(), lit(null))
                        .when(col("ROAM_DIRECTION").equalTo(0), "Inbound")
                        .when(col("ROAM_DIRECTION").equalTo(1), "Outbound")
                        .otherwise(lit("unknown")))

                .withColumn("NI", when(col("NI_source").isNull(), lit(null))
                        .when(col("NI_source").equalTo(0), "International network code")
                        .when(col("NI_source").equalTo(1), "International Spare network code")
                        .when(col("NI_source").equalTo(2), "National network code")
                        .when(col("NI_source").equalTo(3), "National Spare network code")
                        .otherwise(lit("unknown")))

                .withColumn("AuthInfoReqCount", when(col("AuthInfoReqCount_source").isNull(), lit(null))
                        .when(col("AuthInfoReqCount_source").equalTo(0), "Non-authentication transaction")
                        .when(col("AuthInfoReqCount_source").equalTo(1), "Authentication transaction")
                        .otherwise(lit("unknown")))

                .withColumn("AuthInfoRspCount", when(col("AuthInfoRspCount_source").isNull(), lit(null))
                        .when(col("AuthInfoRspCount_source").equalTo(1), "Authentication transaction and successful")
                        .when(col("AuthInfoRspCount_source").equalTo(0), "Not an authentication transaction or not successful")
                        .otherwise(lit("unknown")))

                .withColumn("ISDindCount", when(col("ISDindCount_source").isNull(), lit(null))
                        .when(col("ISDindCount_source").equalTo(1), "Insert Subscriber Data transaction")
                        .when(col("ISDindCount_source").equalTo(0), "Non-Insert Subscriber Data transaction")
                        .otherwise(lit("unknown")))

                .withColumn("ISDRSPCount", when(col("ISDRSPCount_source").isNull(), lit(null))
                        .when(col("ISDRSPCount_source").equalTo(1), "Successful Insert Subscriber Data transaction")
                        .when(col("ISDRSPCount_source").equalTo(0), "Non-Insert Subscriber Data transaction or unsuccessful transaction")
                        .otherwise(lit("unknown")));

        Dataset<Row> tableID = references.get("tableID")
                .withColumnRenamed("Key", "TABLE_ID_Key")
                .withColumnRenamed("Value", "TABLE_ID");
        tableID.persist();

        Dataset<Row> srvType = references.get("srvType")
                .withColumnRenamed("Key", "SRVTYPE_Key")
                .withColumnRenamed("Value", "SRVTYPE");
        srvType.persist();

        Dataset<Row> cause = references.get("cause")
                .withColumnRenamed("Key", "CAUSE_Key")
                .withColumnRenamed("Value", "CAUSE_Label");
        cause.persist();

        Dataset<Row> sigCollectionType = references.get("sigCollectionType")
                .withColumnRenamed("Key", "SIG_COLLECTION_TYPE_Key")
                .withColumnRenamed("Value", "SIG_COLLECTION_TYPE");
        sigCollectionType.persist();

        Dataset<Row> procType = references.get("procType")
                .withColumnRenamed("Key", "PROC_TYPE_Key")
                .withColumnRenamed("Value", "PROC_TYPE_Label");
        procType.persist();

        Dataset<Row> pd = references.get("pd")
                .withColumnRenamed("Key", "PD_Key")
                .withColumnRenamed("Value", "PD");
        pd.persist();

        ds = ds.join(broadcast(tableID), ds.col("TABLE_ID_source").equalTo(tableID.col("TABLE_ID_Key")), "leftouter")
                .join(broadcast(srvType), ds.col("SRVTYPE_source").equalTo(srvType.col("SRVTYPE_Key")), "leftouter")
                .join(broadcast(cause), ds.col("CAUSE").equalTo(cause.col("CAUSE_Key")), "leftouter")
                .join(broadcast(sigCollectionType), ds.col("SIG_COLLECTION_TYPE_source").equalTo(sigCollectionType.col("SIG_COLLECTION_TYPE_Key")), "leftouter")
                .join(broadcast(procType), ds.col("PROC_TYPE").equalTo(procType.col("PROC_TYPE_Key")), "leftouter")
                .join(broadcast(pd), ds.col("PD_source").equalTo(pd.col("PD_Key")), "leftouter");

        ds = ds.withColumn("TABLE_ID", when(col("TABLE_ID_source").isNull(), lit(null)).otherwise(coalesce(col("TABLE_ID"), lit("unknown"))))
                .withColumn("SRVTYPE", when(col("SRVTYPE_source").isNull(), lit(null)).otherwise(coalesce(col("SRVTYPE"), lit("unknown"))))
                .withColumn("CAUSE_Label", when(col("CAUSE").isNull(), lit(null)).otherwise(coalesce(col("CAUSE_Label"), lit("unknown"))))
                .withColumn("SIG_COLLECTION_TYPE", when(col("SIG_COLLECTION_TYPE_source").isNull(), lit(null)).otherwise(coalesce(col("SIG_COLLECTION_TYPE"), lit("unknown"))))
                .withColumn("PROC_TYPE_Label", when(col("PROC_TYPE").isNull(), lit(null)).otherwise(coalesce(col("PROC_TYPE_Label"), lit("unknown"))))
                .withColumn("PD", when(col("PD_source").isNull(), lit(null)).otherwise(coalesce(col("PD"), lit("unknown"))));

        ds = ds.drop("TABLE_ID_source")
                .drop("SRVSTAT_source")
                .drop("NI_source")
                .drop("SRVTYPE_source")
                .drop("SIG_COLLECTION_TYPE_source")
                .drop("PD_source")
                .drop("AuthInfoReqCount_source")
                .drop("AuthInfoRspCount_source")
                .drop("ISDindCount_source")
                .drop("ISDRSPCount_source")
                .drop("TABLE_ID_Key")
                .drop("SRVTYPE_Key")
                .drop("CAUSE_Key")
                .drop("SIG_COLLECTION_TYPE_Key")
                .drop("PROC_TYPE_Key")
                .drop("PD_Key");

        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}