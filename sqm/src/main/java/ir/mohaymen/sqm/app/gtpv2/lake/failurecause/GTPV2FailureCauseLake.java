package ir.mohaymen.sqm.app.gtpv2.lake.failurecause;

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
public class GTPV2FailureCauseLake extends Lake {
    public GTPV2FailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("TABLE_ID", "SID", "PROTOCOL_ID", "INTERFACEID", "IMSI", "MSISDN", "IMEI", "MS_IP", "TMSI",
                "ROAM_DIRECTION", "HOMEMNC", "HOMEMCC", "RAT", "MCC", "MNC", "LAI", "RAI_TAI",
                "SAI_CGI_ECGI", "APN", "PROC_TYPE", "MME_IP_Address", "SGW_SIG_IP", "PGW_SIG_IP", "ENODEB_DATA_IP",
                "SGW_DATA_IP", "PGW_DATA_IP", "ENODEB_DATA_TEID", "MME_SIG_TEID", "SGW_SIG_TEID", "PGW_SIG_TEID",
                "SGW_DATA_TEID", "PGW_DATA_TEID", "SIG_DIRECTION", "TRANS_TYPE",
                "TRANS_REQ_TIME_SEC", "TRANS_REQ_TIME_MSEC", "TRANS_RSP_TIME_SEC", "TRANS_RSP_TIME_MSEC",
                "TRANS_SUCCED_FLAG", "TRANS_RESELECT_FLAG", "TRANS_CAUSE", "LBI", "EBI",
                "DST_MME_SIG_IP", "MBR_DL_NEG", "MBR_UL_NEG", "QCI_NEG", "ARP_NEG", "SV", "PDN_TYPE", "Probe_ID",
                "DCNR", "RECORD_DATE_TEHRAN", "TAC", "MME_Name", "MME_SIG_IP_Ref", "SGW", "PGW", "Home_Region",
                "Home_Country", "Home_Operator", "Home_Roaming_Type", "Country", "Region", "Province", "City", "SITE",
                "CELL", "Brand", "Model", "Technology", "Type", "OS", "RECORD_DATE", "Visited_Roaming_Type",
                "Visited_Country", "Visited_Operator");
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("INTERFACEID").equalTo(6)
                .and(col("TRANS_SUCCED_FLAG").equalTo(1))
                        .and(col("Visited_Country").equalTo("IRAQ (418)")));

        ds = ds.withColumnRenamed("TABLE_ID", "TABLE_ID_source")
                .withColumnRenamed("PROTOCOL_ID", "PROTOCOL_ID_source")
                .withColumnRenamed("INTERFACEID", "INTERFACEID_source")
                .withColumnRenamed("RAT", "RAT_source")
                .withColumnRenamed("PROC_TYPE", "PROC_TYPE_source")
                .withColumnRenamed("SIG_DIRECTION", "SIG_DIRECTION_source")
                .withColumnRenamed("TRANS_SUCCED_FLAG", "TRANS_SUCCED_FLAG_source")
                .withColumnRenamed("TRANS_RESELECT_FLAG", "TRANS_RESELECT_FLAG_source")

                .withColumn("PROTOCOL_ID", when(col("PROTOCOL_ID_source").isNull(), lit(null))
                        .when(col("PROTOCOL_ID_source").equalTo(6), "GTP V2")
                        .otherwise(lit("unknown")))
                .withColumn("SIG_DIRECTION", when(col("SIG_DIRECTION_source").isNull(), lit(null))
                        .when(col("SIG_DIRECTION_source").equalTo(1), "LTEtoUTRAN")
                        .when(col("SIG_DIRECTION_source").equalTo(2), "LTEtoGERAN")
                        .when(col("SIG_DIRECTION_source").equalTo(3), "UTRANtoLTE")
                        .when(col("SIG_DIRECTION_source").equalTo(4), "GERANtoLTE")
                        .otherwise(lit("unknown")))
                .withColumn("TRANS_SUCCED_FLAG", when(col("TRANS_SUCCED_FLAG_source").isNull(), lit(null))
                        .when(col("TRANS_SUCCED_FLAG_source").equalTo(0), "Successful")
                        .when(col("TRANS_SUCCED_FLAG_source").equalTo(1), "Failed")
                        .otherwise(lit("unknown")))
                .withColumn("TRANS_RESELECT_FLAG", when(col("TRANS_RESELECT_FLAG_source").isNull(), lit(null))
                        .when(col("TRANS_RESELECT_FLAG_source").equalTo(0), "Not retransmitted")
                        .when(col("TRANS_RESELECT_FLAG_source").equalTo(1), "Retransmitted")
                        .otherwise(lit("unknown")))
                .withColumn("ROAM_DIRECTION_Label", when(col("ROAM_DIRECTION").isNull(), lit(null))
                        .when(col("ROAM_DIRECTION").equalTo(0), "Inbound")
                        .when(col("ROAM_DIRECTION").equalTo(1), "Outbound")
                        .otherwise(lit("unknown")))
                .withColumn("INTERFACEID", when(col("INTERFACEID_source").isNull(), lit(null))
                        .when(col("INTERFACEID_source").equalTo(6), "S5/S8")
                        .when(col("INTERFACEID_source").equalTo(5), "S11")
                        .when(col("INTERFACEID_source").equalTo(10), "S3/S10/S16")
                        .when(col("INTERFACEID_source").equalTo(14), "S4/S12")
                        .when(col("INTERFACEID_source").equalTo(32), "S2b")
                        .otherwise(lit("unknown")));

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
                .withColumnRenamed("INTERFACEID", "Trans_Type_INTERFACEID_Key")
                .withColumnRenamed("Value", "TRANS_TYPE_Label");
        transType.persist();

        Dataset<Row> transCause = references.get("transCause")
                .withColumnRenamed("Key", "TRANS_CAUSE_Label_Key")
                .withColumnRenamed("Value", "TRANS_CAUSE_Label");
        transCause.persist();

        ds = ds.join(broadcast(tableID), ds.col("TABLE_ID_source").equalTo(tableID.col("TABLE_ID_Key")), "leftouter")
                .join(broadcast(rat), ds.col("RAT_source").equalTo(rat.col("RAT_Key")), "leftouter")
                .join(broadcast(procType), ds.col("PROC_TYPE_source").equalTo(procType.col("PROC_TYPE_Key")), "leftouter")
                .join(broadcast(transType), ds.col("TRANS_TYPE").equalTo(transType.col("TRANS_TYPE_Key")).
                        and(ds.col("INTERFACEID_source").equalTo(transType.col("Trans_Type_INTERFACEID_Key"))), "leftouter")
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
                .drop("TRANS_RESELECT_FLAG_source")

                .drop("TABLE_ID_Key")
                .drop("RAT_Key")
                .drop("PROC_TYPE_Key")
                .drop("TRANS_TYPE_Key")
                .drop("TRANS_CAUSE_Label_Key")
                .drop("Trans_Type_INTERFACEID_Key");

        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}

