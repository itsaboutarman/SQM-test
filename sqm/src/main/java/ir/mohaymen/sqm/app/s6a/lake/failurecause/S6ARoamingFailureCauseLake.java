package ir.mohaymen.sqm.app.s6a.lake.failurecause;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;
import java.util.Map;
import static org.apache.spark.sql.functions.*;
import static java.util.stream.Collectors.joining;
import java.util.Arrays;
import java.util.List;

@Job
public class S6ARoamingFailureCauseLake extends Lake {
    public S6ARoamingFailureCauseLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE", "Table_id", "SID", "PROTOCOL_ID", "INTERFACEID", "IMSI", "MSISDN", "IMEI",
                "ROAM_DIRECTION", "HOMEMCC", "HOMEMNC", "MCC", "MNC", "PROC_TYPE", "HSS_SIG_IP", "MME_IP_Address",
                "TRANS_TYPE", "TRANS_REQ_TIME_SEC", "TRANS_REQ_TIME_MSEC", "TRANS_RSP_TIME_SEC", "TRANS_RSP_TIME_MSEC",
                "TRANS_SUCCED_FLAG", "TRANS_RESELECT_FLAG", "TRANS_CAUSE_TYPE", "TRANS_CAUSE", "RESYN_INFO_FLAG",
                "UE_SRVCC_CAPABILITY", "FAIL_CATEGORY", "ORIGIN_HOST", "TADS_FLAG", "ORIGIN_REALM", "DESTINATION_HOST",
                "DESTINATION_REALM", "STN_SR", "Probe_ID", "RESERVED2", "DCNR_RESTRICT", "NETWORK_SUPPORT_DCNR",
                "RECORD_DATE_TEHRAN", "TAC", "MME_Name", "HSS_Name", "HSS_IP", "Brand", "Model", "Technology",
                "Type", "OS", "Region", "Visited_Country", "Visited_Operator", "Visited_Roaming_Type",
                "Home_Country", "Home_Operator", "Home_Roaming_Type");
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.where(col("TRANS_SUCCED_FLAG").equalTo(1));

        ds = ds.withColumnRenamed("Table_id", "Table_id_source")
                .withColumnRenamed("PROTOCOL_ID", "PROTOCOL_ID_source")
                .withColumnRenamed("INTERFACEID", "INTERFACEID_source")
                .withColumnRenamed("PROC_TYPE", "PROC_TYPE_source")
                .withColumnRenamed("TRANS_SUCCED_FLAG", "TRANS_SUCCED_FLAG_source")
                .withColumnRenamed("TRANS_RESELECT_FLAG", "TRANS_RESELECT_FLAG_source")
                .withColumnRenamed("RESYN_INFO_FLAG", "RESYN_INFO_FLAG_source")
                .withColumnRenamed("UE_SRVCC_CAPABILITY", "UE_SRVCC_CAPABILITY_source")
                .withColumnRenamed("TADS_FLAG", "TADS_FLAG_source")

                .withColumn("PROTOCOL_ID", when(col("PROTOCOL_ID_source").equalTo(8), "Diameter"))
                .withColumn("INTERFACEID", when(col("INTERFACEID_source").equalTo(8), "S6a")
                        .when(col("INTERFACEID_source").equalTo(66), "S13"))
                .withColumn("ROAM_DIRECTION_Label", when(col("ROAM_DIRECTION").equalTo(0), "Inbound")
                        .when(col("ROAM_DIRECTION").equalTo(1), "Outbound"))
                .withColumn("TRANS_SUCCED_FLAG", when(col("TRANS_SUCCED_FLAG_source").equalTo(0), "Successful")
                        .when(col("TRANS_SUCCED_FLAG_source").equalTo(1), "Failed"))
                .withColumn("TRANS_RESELECT_FLAG", when(col("TRANS_RESELECT_FLAG_source").equalTo(0), "Not retransmitted")
                        .when(col("TRANS_RESELECT_FLAG_source").equalTo(1), "Retransmitted"))
                .withColumn("TRANS_CAUSE_TYPE_Label", when(col("TRANS_CAUSE_TYPE").equalTo(0), "Diameter Result-Code")
                        .when(col("TRANS_CAUSE_TYPE").equalTo(1), "Diameter Experimental-Result-Code")
                        .when(col("TRANS_CAUSE_TYPE").equalTo(255), "Unknown"))
                .withColumn("RESYN_INFO_FLAG", when(col("RESYN_INFO_FLAG_source").equalTo(0), "Authentication resynchronization does not occur.")
                        .when(col("RESYN_INFO_FLAG_source").equalTo(1), "Authentication resynchronization occurs."))
                .withColumn("UE_SRVCC_CAPABILITY", when(col("UE_SRVCC_CAPABILITY_source").equalTo(0), "UE-SRVCC-NOT-SUPPORTED")
                        .when(col("UE_SRVCC_CAPABILITY_source").equalTo(1), "UE-SRVCC-SUPPORTED"))
                .withColumn("TADS_FLAG", when(col("TADS_FLAG_source").equalTo(0), "set")
                        .when(col("TADS_FLAG_source").equalTo(1), "Not set"));

        Dataset<Row> tableID = references.get("tableID")
                .withColumnRenamed("Key", "Table_id_Key")
                .withColumnRenamed("Value", "Table_id");
        tableID.persist();

        Dataset<Row> procType = references.get("procType")
                .withColumnRenamed("Key", "PROC_TYPE_Key")
                .withColumnRenamed("Value", "PROC_TYPE");
        procType.persist();

        Dataset<Row> transType = references.get("transType")
                .withColumnRenamed("Key", "TRANS_TYPE_Key")
                .withColumnRenamed("Value", "TRANS_TYPE_Label");
        transType.persist();

        Dataset<Row> transCause  = references.get("transCause")
                .withColumnRenamed("Key", "TRANS_CAUSE_Key")
                .withColumnRenamed("Value", "TRANS_CAUSE_Label");
        transCause.persist();


        ds =  ds.join(broadcast(tableID), ds.col("Table_id_source").equalTo(tableID.col("Table_id_Key")), "leftouter")
                .join(broadcast(procType), ds.col("PROC_TYPE_source").equalTo(procType.col("PROC_TYPE_Key")), "leftouter")
                .join(broadcast(transType), ds.col("TRANS_TYPE").equalTo(transType.col("TRANS_TYPE_Key")), "leftouter")
                .join(broadcast(transCause), ds.col("TRANS_CAUSE").equalTo(transCause.col("TRANS_CAUSE_Key"))
                        .and(ds.col("TRANS_CAUSE_TYPE").equalTo(transCause.col("AVP_Type_Code"))), "leftouter");


        ds = ds.drop("Table_id_source")
                .drop("PROTOCOL_ID_source")
                .drop("INTERFACEID_source")
                .drop("PROC_TYPE_source")
                .drop("TRANS_SUCCED_FLAG_source")
                .drop("TRANS_RESELECT_FLAG_source")
                .drop("RESYN_INFO_FLAG_source")
                .drop("UE_SRVCC_CAPABILITY_source")
                .drop("TADS_FLAG_source")
                .drop("AVP_Type_Code")

                .drop("TRANS_TYPE_Key")
                .drop("TRANS_CAUSE_Key")
                .drop("Table_id_Key")
                .drop("PROC_TYPE_Key");

        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}

