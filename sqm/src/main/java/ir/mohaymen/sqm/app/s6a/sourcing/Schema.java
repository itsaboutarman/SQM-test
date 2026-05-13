package ir.mohaymen.sqm.app.s6a.sourcing;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schema extends ir.mohaymen.sqm.app.Schema {


    public static StructType getSchema() {

        return new StructType()
                .add("Table_id", DataTypes.IntegerType, true)
                .add("SID", DataTypes.LongType, true)
                .add("PROTOCOL_ID", DataTypes.IntegerType, true)
                .add("INTERFACEID", DataTypes.IntegerType, true)
                .add("IMSI", DataTypes.StringType, true)
                .add("MSISDN", DataTypes.StringType, true)
                .add("IMEI", DataTypes.StringType, true)
                .add("ROAM_DIRECTION", DataTypes.IntegerType, true)
                .add("HOMEMCC", DataTypes.StringType, true)
                .add("HOMEMNC", DataTypes.StringType, true)
                .add("MCC", DataTypes.StringType, true)
                .add("MNC", DataTypes.StringType, true)
                .add("PROC_TYPE", DataTypes.IntegerType, true)
                .add("HSS_SIG_IP", DataTypes.StringType, true)
                .add("MME_SIG_IP", DataTypes.StringType, true)
                .add("TRANS_TYPE", DataTypes.IntegerType, true)
                .add("TRANS_REQ_TIME_SEC", DataTypes.IntegerType, true)
                .add("TRANS_REQ_TIME_MSEC", DataTypes.IntegerType, true)
                .add("TRANS_RSP_TIME_SEC", DataTypes.IntegerType, true)
                .add("TRANS_RSP_TIME_MSEC", DataTypes.IntegerType, true)
                .add("TRANS_SUCCED_FLAG", DataTypes.IntegerType, true)
                .add("TRANS_RESELECT_FLAG", DataTypes.IntegerType, true)
                .add("TRANS_CAUSE_TYPE", DataTypes.IntegerType, true)
                .add("TRANS_CAUSE", DataTypes.IntegerType, true)
                .add("RESYN_INFO_FLAG", DataTypes.IntegerType, true)
                .add("UE_SRVCC_CAPABILITY", DataTypes.IntegerType, true)
                .add("FAIL_CATEGORY", DataTypes.IntegerType, true)
                .add("ORIGIN_HOST", DataTypes.StringType, true)
                .add("TADS_FLAG", DataTypes.IntegerType, true)
                .add("ORIGIN_REALM", DataTypes.StringType, true)
                .add("DESTINATION_HOST", DataTypes.StringType, true)
                .add("DESTINATION_REALM", DataTypes.StringType, true)
                .add("col33", DataTypes.StringType, true)
                .add("col34", DataTypes.StringType, true)
                .add("col35", DataTypes.StringType, true);
    }
}
