package ir.mohaymen.sqm.app.gtpv0v1.lake.failurecause;

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
public class GTPV0V1FailureCauseLake extends Lake {
    public GTPV0V1FailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {

        return ds.select("TABLE_ID", "SID", "PROTOCOL_ID", "INTERFACEID", "IMSI", "MSISDN", "IMEI", "MS_IP", "PTMSI",
                "ROAM_DIRECTION", "HOMEMNC", "HOMEMCC", "RAT", "MCC", "MNC", "LAI", "RAI_TAI",
                "SAI_CGI_ECGI", "APN", "PROC_TYPE", "SGSN_SIG_IP", "GGSN_SIG_IP", "SGSN_DATA_IP", "GGSN_DATA_IP",
                "SGSN_SIG_TEID", "GGSN_SIG_TEID", "SGSN_DATA_TEID", "GGSN_DATA_TEID", "oldSGSN_SIG_TEID",
                "oldSGSN_SIG_IP", "TRANS_TYPE", "SIG_DIRECTION", "TRANS_REQ_TIME_SEC",
                "TRANS_REQ_TIME_MSEC", "TRANS_RSP_TIME_SEC", "TRANS_RSP_TIME_MSEC", "TRANS_SUCCED_FLAG",
                "TRANS_CAUSE", "ACT_TYPE", "NSAPI", "UL_VOLOME", "DL_VOLOME", "MBR_DL_NEG",
                "MBR_UL_NEG", "TRAFFIC_CLASS_NEG", "THP_NEG", "ARP_NEG", "SV", "PDP_TYPE", "MS_IP2",
                "RECORD_DATE_TEHRAN","RECORD_DATE", "TAC", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                "Home_Region", "Home_Country", "Home_Operator", "Home_Roaming_Type", "Brand", "Model", "Technology",
                "Type", "OS");

    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("INTERFACEID").isin(1, 13)
                        .and(col("TRANS_SUCCED_FLAG").equalTo(1)))
                .drop("offset")
                .drop("partition")
                .drop("timestamp")
                .drop("day");

        ds = ds.withColumnRenamed("TABLE_ID", "TABLE_ID_source")
                .withColumnRenamed("PROTOCOL_ID", "PROTOCOL_ID_source")
                .withColumnRenamed("INTERFACEID", "INTERFACEID_source")
                .withColumnRenamed("RAT", "RAT_source")
                .withColumnRenamed("PROC_TYPE", "PROC_TYPE_source")
                .withColumnRenamed("SIG_DIRECTION", "SIG_DIRECTION_source")
                .withColumnRenamed("TRANS_SUCCED_FLAG", "TRANS_SUCCED_FLAG_source")
                .withColumnRenamed("ACT_TYPE", "ACT_TYPE_source")

                .withColumn("PROTOCOL_ID",
                        when(col("PROTOCOL_ID_source").isNull(), lit(null))
                                .when(col("PROTOCOL_ID_source").equalTo(1), lit("GTP V1"))
                                .when(col("PROTOCOL_ID_source").equalTo(13), lit("GTPv0"))
                                .otherwise(lit("unknown"))
                )
                .withColumn("ROAM_DIRECTION_Label",
                        when(col("ROAM_DIRECTION").isNull(), lit(null))
                                .when(col("ROAM_DIRECTION").equalTo(0), lit("Inbound"))
                                .when(col("ROAM_DIRECTION").equalTo(1), lit("Outbound"))
                                .otherwise(lit("unknown"))
                )
                .withColumn("SIG_DIRECTION",
                        when(col("SIG_DIRECTION_source").isNull(), lit(null))
                                .when(col("SIG_DIRECTION_source").equalTo(0), lit("SGSN->GGSN"))
                                .when(col("SIG_DIRECTION_source").equalTo(1), lit("GGSN->SGSN"))
                                .otherwise(lit("unknown"))
                )
                .withColumn("TRANS_SUCCED_FLAG",
                        when(col("TRANS_SUCCED_FLAG_source").isNull(), lit(null))
                                .when(col("TRANS_SUCCED_FLAG_source").equalTo(0), lit("Successful"))
                                .when(col("TRANS_SUCCED_FLAG_source").equalTo(1), lit("Failed"))
                                .otherwise(lit("unknown"))
                )
                .withColumn("ACT_TYPE",
                        when(col("ACT_TYPE_source").isNull(), lit(null))
                                .when(col("ACT_TYPE_source").equalTo(0), lit("Primary activation"))
                                .when(col("ACT_TYPE_source").equalTo(1), lit("Secondary activation"))
                                .otherwise(lit("unknown"))
                )
                .withColumn("INTERFACEID",
                        when(col("INTERFACEID_source").isNull(), lit(null))
                                .when(col("INTERFACEID_source").equalTo(1), lit("Gp/Gn"))
                                .otherwise(lit("unknown"))
                );

        Dataset<Row> tableID = references.get("tableID")
                .withColumnRenamed("Key", "TABLE_ID_Key")
                .withColumnRenamed("Value", "TABLE_ID");
        tableID.persist();

        Dataset<Row> rat = references.get("rat")
                .withColumnRenamed("Key", "RAT_Key")
                .withColumnRenamed("Value", "RAT");
        rat.persist();

        Dataset<Row> procType = references.get("procType")
                .withColumnRenamed("Key", "PROC_TYPE_Key")
                .withColumnRenamed("Value", "PROC_TYPE");
        procType.persist();

        Dataset<Row> transType = references.get("transType")
                .withColumnRenamed("Key", "TRANS_TYPE_Key")
                .withColumnRenamed("Value", "TRANS_TYPE_Label");
        transType.persist();

        Dataset<Row> transCause = references.get("transCause")
                .withColumnRenamed("Key", "TRANS_CAUSE_Label_Key")
                .withColumnRenamed("Value", "TRANS_CAUSE_Label");
        transCause.persist();

        ds = ds.join(broadcast(tableID), ds.col("TABLE_ID_source").equalTo(tableID.col("TABLE_ID_Key")), "leftouter")
                .join(broadcast(rat), ds.col("RAT_source").equalTo(rat.col("RAT_Key")), "leftouter")
                .join(broadcast(procType), ds.col("PROC_TYPE_source").equalTo(procType.col("PROC_TYPE_Key")), "leftouter")
                .join(broadcast(transType), ds.col("TRANS_TYPE").equalTo(transType.col("TRANS_TYPE_Key")), "leftouter")
                .join(broadcast(transCause), ds.col("TRANS_CAUSE").equalTo(transCause.col("TRANS_CAUSE_Label_Key")), "leftouter");

        ds = ds.withColumn("TABLE_ID",
                        when(col("TABLE_ID_source").isNull(), lit(null))
                                .otherwise(coalesce(col("TABLE_ID"), lit("unknown")))
                )
                .withColumn("RAT",
                        when(col("RAT_source").isNull(), lit(null))
                                .otherwise(coalesce(col("RAT"), lit("unknown")))
                )
                .withColumn("PROC_TYPE",
                        when(col("PROC_TYPE_source").isNull(), lit(null))
                                .otherwise(coalesce(col("PROC_TYPE"), lit("unknown")))
                )
                .withColumn("TRANS_TYPE_Label",
                        when(col("TRANS_TYPE").isNull(), lit(null))
                                .otherwise(coalesce(col("TRANS_TYPE_Label"), lit("unknown")))
                )
                .withColumn("TRANS_CAUSE_Label",
                        when(col("TRANS_CAUSE").isNull(), lit(null))
                                .otherwise(coalesce(col("TRANS_CAUSE_Label"), lit("unknown")))
                );

        ds = ds.drop("TABLE_ID_source")
                .drop("PROTOCOL_ID_source")
                .drop("INTERFACEID_source")
                .drop("RAT_source")
                .drop("PROC_TYPE_source")
                .drop("SIG_DIRECTION_source")
                .drop("TRANS_SUCCED_FLAG_source")
                .drop("ACT_TYPE_source")

                .drop("TABLE_ID_Key")
                .drop("RAT_Key")
                .drop("PROC_TYPE_Key")
                .drop("TRANS_TYPE_Key")
                .drop("TRANS_CAUSE_Label_Key");

        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}