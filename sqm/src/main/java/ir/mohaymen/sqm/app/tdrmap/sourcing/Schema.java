package ir.mohaymen.sqm.app.tdrmap.sourcing;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schema extends ir.mohaymen.sqm.app.Schema {


    public static StructType getSchema() {

        return new StructType()
                .add("Table_id", DataTypes.IntegerType, true)
                .add("TDRID", DataTypes.StringType, true)
                .add("STARTTIME", DataTypes.IntegerType, true)
                .add("MILLISEC", DataTypes.IntegerType, true)
                .add("SRVSTAT", DataTypes.IntegerType, true)
                .add("NI", DataTypes.IntegerType, true)
                .add("OPC", DataTypes.IntegerType, true)
                .add("OSSN", DataTypes.IntegerType, true)
                .add("OGT", DataTypes.StringType, true)
                .add("DPC", DataTypes.IntegerType, true)
                .add("DSSN", DataTypes.IntegerType, true)
                .add("DGT", DataTypes.StringType, true)
                .add("SRVTYPE", DataTypes.IntegerType, true)
                .add("HLRNO", DataTypes.StringType, true)
                .add("SGSNNO", DataTypes.StringType, true)
                .add("SGSNIP", DataTypes.IntegerType, true)
                .add("MSISDN", DataTypes.StringType, true)
                .add("IMSI", DataTypes.StringType, true)
                .add("TMSI", DataTypes.StringType, true)
                .add("IMEI", DataTypes.StringType, true)
                .add("ROAM_DIRECTION", DataTypes.IntegerType, true)
                .add("HOMEMCC", DataTypes.StringType, true)
                .add("HOMEMNC", DataTypes.StringType, true)
                .add("VISITCC", DataTypes.StringType, true)
                .add("VISITNDC", DataTypes.StringType, true)
                .add("SRVRSPTIME", DataTypes.IntegerType, true)
                .add("END_TIME", DataTypes.IntegerType, true)
                .add("REL_TYPE", DataTypes.IntegerType, true)
                .add("CAUSE", DataTypes.IntegerType, true)
                .add("SIG_COLLECTION_TYPE", DataTypes.IntegerType, true)
                .add("PROC_TYPE", DataTypes.IntegerType, true)
                .add("PD", DataTypes.IntegerType, true)
                .add("AuthInfoReqCount", DataTypes.IntegerType, true)
                .add("AuthInfoRspCount", DataTypes.IntegerType, true)
                .add("AuthInfoDelay", DataTypes.IntegerType, true)
                .add("ISDindCount", DataTypes.IntegerType, true)
                .add("ISDRSPCount", DataTypes.IntegerType, true)
                .add("ISDDELAY", DataTypes.IntegerType, true)
                .add("col33", DataTypes.StringType, true)
                .add("col34", DataTypes.StringType, true)
                .add("col35", DataTypes.StringType, true);
    }
}
