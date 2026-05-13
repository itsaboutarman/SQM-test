package ir.mohaymen.sqm.app.s1mme.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class S1MMESourcing extends Sourcing {
    public S1MMESourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

//    private Dataset<Row> mapColumnUsingRef(Dataset<Row> ds, Dataset<Row> ref, Column joinKeyCol, String refJoinKeyCol
//            , String refValueCol, String mappedColName, Column defaultValue) {
//        ds = ds.join(
//                        broadcast(ref.as("map_ref"))
//                        , joinKeyCol.equalTo(col("map_ref." + refJoinKeyCol))
//                        , "leftouter")
//                .drop(refJoinKeyCol);
//        if(defaultValue != null)
//            ds = ds.withColumn(mappedColName, coalesce(col("map_ref." + refValueCol), defaultValue))
//                    .drop(refValueCol);
//        else
//            ds = ds.withColumnRenamed(mappedColName, "map_ref." + refValueCol);
//        return ds;
//    }

    private Dataset<Row> applyOneToOneMappings(Dataset<Row> ds, HashMap<String, HashMap<Integer, String>> mappings) {
        for (Map.Entry<String, HashMap<Integer, String>> set :
                mappings.entrySet()) {
            String[] names = set.getKey().split("\\|");
            Column cond = null;
            Column colToMap = col(names[0]);
            for(Map.Entry<Integer, String> entry : set.getValue().entrySet()) {
                Integer key = entry.getKey();
                if(key == null)
                    cond = cond == null ? when(colToMap.isNull(), entry.getValue())
                            : cond.when(colToMap.isNull(), entry.getValue());
                else
                    cond = cond == null ? when(colToMap.equalTo(key), entry.getValue())
                            : cond.when(colToMap.equalTo(key), entry.getValue());
            }
            assert cond != null;
            ds = ds.withColumn(names[1], cond.otherwise(colToMap));
            if(!names[0].equals(names[1]))
                ds = ds.drop(names[0]);
        }
        return ds;
    }

    private HashMap<String, HashMap<Integer, String>> getOneToOneMappings() {
        HashMap<String, HashMap<Integer, String>> mappings = new HashMap<String, HashMap<Integer, String>>();
        HashMap<Integer, String> mapValues = new HashMap<>();
        mapValues.put(100, "Attach");
        mapValues.put(101, "UE_Initiated_Detach");
        mapValues.put(102, "Network_Initiated_Detach");
        mapValues.put(103, "Tracking_Area_Updating");
        mapValues.put(104, "NW_Triggered_Service_Request");
        mapValues.put(105, "UE_Triggered_Service_Request");
        mapValues.put(106, "UE_requested_PDN_connectivity");
        mapValues.put(107, "UE_requested_PDN_disconnect");
        mapValues.put(108, "UE_requested_bearer_resource_allocation");
        mapValues.put(109, "UE_requested_bearer_resource_modification");
        mapValues.put(110, "Network_Initiated_Dedicated_EPS_bearer_context_activation");
        mapValues.put(111, "Network_Initiated_EPS_bearer_context_modification");
        mapValues.put(112, "Network_Initiated_EPS_bearer_context_deactivation");
        mapValues.put(113, "X2_Based_Handover");
        mapValues.put(114, "S1_Based_Handover");
        mapValues.put(115, "E_RAB_Release");
        mapValues.put(116, "UE_Context_Release");
        mapValues.put(119, "SMS_Send");
        mapValues.put(120, "SMS_Receive");
        mapValues.put(126, "E_RAB_ModificationIndication");
        mapValues.put(254, "Others");
        mappings.put("PROCEDURE_TYPE|ProcedureType", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(3003, "CDR_S1MME");
//        mapValues.put(3001, "CDR_GbIuPS");
//        mapValues.put(3002, "CDR_IuPsRelease");
//        mapValues.put(3004, "CDR_S1Handover");
//        mapValues.put(3005, "CDR_S1Release");
//        mapValues.put(3006, "CDR_S1OTHER");
//        mapValues.put(3007, "CDR_GTPV2");
//        mapValues.put(3008, "CDR_S6a");
//        mapValues.put(3009, "CDR_SGS");
//        mapValues.put(3010, "CDR_Sv");
//        mapValues.put(3011, "CDR_Radius");
//        mapValues.put(3012, "CDR_SGiDiameter");
//        mapValues.put(3013, "TDR_MAP_GMM");
//        mapValues.put(3014, "CDR_GTPV0V1");
//        mappings.put("Table_ID|Table_ID", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(7, "S1AP");
//        mappings.put("PROTOCOL_ID|PROTOCOL_ID", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(7, "S1-MME");
//        mappings.put("INTERFACEID|INTERFACEID", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(6, "EUTRAN");
        mappings.put("RAT|RAT", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "Successful");
//        mapValues.put(1, "Failed");
//        mapValues.put(2, "Timed-out");
//        mappings.put("PROCEDURE_SUCCEED_FLAG|ProcedureStatus", mapValues); //was incorrectly named in doc

        mapValues = new HashMap<>();
        mapValues.put(48, "EMM_Attach");
        mapValues.put(49, "EMM_Detach");
        mapValues.put(50, "EMM_GUTI_reallocation");
        mapValues.put(54, "EMM_Tracking_area_update");
        mapValues.put(57, "EMM_Service_Request");
        mapValues.put(58, "EMM_Extended_Service");
        mapValues.put(62, "EMM_UPLINK_NAS_TRANSPORT");
        mapValues.put(60, "EMM_DOWNLINK_NAS_TRANSPORT");
        mapValues.put(59, "EMM_CS_SERVICE_NOTIFICATION");
        mapValues.put(61, "EMM_SECURITY_PROTECTED_NAS_MESSAGE");
        mappings.put("EMM_TRANS_TYPE|EMM_Transaction", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"eNodeB-initiated");
//        mapValues.put(1, "MME-initiated");
//        mappings.put("EMM_DIRECTION|EMM_DIRECTION", mapValues); //was incorrectly named DIERECTION in doc

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Inactivated bearer");
//        mapValues.put(1, "Activated bearer");
//        mappings.put("BEARER_ACTIVE_FLAG|BEARER_ACTIVE_FLAG", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Not activate the bearer in the TAU procedure");
//        mapValues.put(1, "Activate the bearer in the TAU procedure");
//        mappings.put("ACTIVE_FLAG|ACTIVE_FLAG", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Successful");
//        mapValues.put(1, "Rejected");
//        mapValues.put(2, "Failed");
//        mappings.put("AUTH_SUCCEED_FLAG|Auth_Status", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(65, "PDN_Connectivity");
        mapValues.put(69, "Bearer_Resource_Modification");
        mapValues.put(71, "PDN_Disconnection");
        mapValues.put(73, "BEARER_RESOURCE_ALLOCATION");
        mapValues.put(76, "ESM_SMS_Send");
        mapValues.put(77, "ESM_SMS_Receive");
        mappings.put("ESM_UE_TRANS_TYPE|UE_ESM_Transaction", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(66, "ACTIVATE_DEFAULT_EPS_BEARER_CONTEXT_REQUEST");
        mapValues.put(67, "Activate_Dedicated_EPS_Bearer_Context_Request");
        mapValues.put(68, "Modify_EPS_Bearer_Context_Request");
        mapValues.put(70, "Deactivate_EPS_Bearer_Context_Request");
        mapValues.put(76, "ESM_SMS_Send");
        mapValues.put(77, "ESM_SMS_Receive");
        mappings.put("ESM_NW_TRANS_TYPE|NW_ESM_Transaction", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(0,"Successful");
        mapValues.put(1, "Rejected");
        mappings.put("PAGING_REQ_SUCCEED_FLAG|Paging_Status", mapValues);
        mappings.put("S1AP_SUCCEED_FLAG|S1AP_Status", mapValues);
        mappings.put("ESM_NW_SUCCEED_FLAG|NW_ESM_Status", mapValues);
        mappings.put("ESM_UE_SUCCEED_FLAG|UE_ESM_Status", mapValues);
//        mappings.put("IDENTITY_SUCCEED_FLAG|IdentityStatus", mapValues);
//        mappings.put("SECURITY_MODE_SUCCED_FLAG|SecurityStatus", mapValues);
        mappings.put("EMM_REQ_SUCCEED_FLAG|EMM_Status", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Not retransmitted");
//        mapValues.put(1, "Retransmitted");
//        mappings.put("PAGING_REQ_RETRANS_FLAG|PAGING_REQ_RETRANS_FLAG", mapValues);
//        mappings.put("S1AP_RETRANS_FLAG|S1AP_RETRANS_FLAG", mapValues);
//        mappings.put("ESM_NW_RETRANS_FLAG|ESM_NW_RETRANS_FLAG", mapValues);
//        mappings.put("ESM_UE_RETRANS_FLAG|ESM_UE_RETRANS_FLAG", mapValues);
//        mappings.put("EMM_REQ_RETRANS_FLAG|EMM_REQ_RETRANS_FLAG", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Not timed out");
//        mapValues.put(1, "Timed out");
//        mappings.put("PAGING_REQ_TIMEOUT_FLAG|Paging_Timeout_Status", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(3, "Initial_Context_Setup");
        mapValues.put(6, "UE_Context_Modification");
        mapValues.put(9, "E-RAB_Setup");
        mapValues.put(10, "E-RAB_Modify");
        mapValues.put(84, "E_RAB_ModificationIndication");
        mappings.put("S1AP_TRANS_TYPE|S1AP_Transaction", mapValues);
//
//        mapValues = new HashMap<>();
//        mapValues.put(0, "Radio_Network_Layer_Cause");
//        mapValues.put(1, "Transport_Layer_Cause");
//        mapValues.put(2, "NAS_Cause");
//        mapValues.put(3, "Protocol_Cause");
//        mapValues.put(4, "Miscellaneous_Cause");
//        mapValues.put(255, "_other");
//        mappings.put("S1AP_CAUSE_TYPE|S1AP_CAUSE_TYPE", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "Non_CSFB_Transaction");
//        mapValues.put(1, "CSFB_ESR_Service_Initiation");
//        mapValues.put(null, "Non_CSFB_Transaction");
//        mappings.put("CSFB_IND|CSFB_Indicator", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "UE_SRVCC_NOT_SUPPORTED");
//        mapValues.put(1, "UE_SRVCC_SUPPORTED");
//        mappings.put("UE_SRVCC_CAPABILITY|UE_SRVCC_CAPABILITY", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "CS_fallback_rejected_by_the_UE");
//        mapValues.put(1, "CS_fallback_accepted_by_the_UE");
//        mappings.put("CSFB_RESPONSE|CSFB_RESPONSE", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(48, "EMM_Attach");
        mapValues.put(49, "EMM_Detach");
        mapValues.put(50, "EMM_GUTI_reallocation");
        mapValues.put(54, "EMM_Tracking_area_update");
        mapValues.put(57, "EMM_Service_Request");
        mapValues.put(58, "EMM_Extended_Service");
        mapValues.put(59, "EMM_CS_SERVICE_NOTIFICATION");
        mapValues.put(60, "EMM_DOWNLINK_NAS_TRANSPORT");
        mapValues.put(61, "EMM_SECURITY_PROTECTED_NAS_MESSAGE");
        mapValues.put(62, "EMM_UPLINK_NAS_TRANSPORT");
        mapValues.put(65, "PDN_Connectivity");
        mapValues.put(66, "ACTIVATE_DEFAULT_EPS_BEARER_CONTEXT_REQUEST");
        mapValues.put(67, "Activate_Dedicated_EPS_Bearer_Context_Request");
        mapValues.put(68, "Modify_EPS_Bearer_Context_Request");
        mapValues.put(69, "Bearer_Resource_Modification");
        mapValues.put(70, "Deactivate_EPS_Bearer_Context_Request");
        mapValues.put(71, "PDN_Disconnection");
        mapValues.put(73, "BEARER_RESOURCE_ALLOCATION");
        mapValues.put(76, "ESM_SMS_Send");
        mapValues.put(77, "ESM_SMS_Receive");
        mappings.put("FIR_FAIL_TRANS|FIR_FAIL_TRANS", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(0, "Radio_Network_Layer_Cause");
        mapValues.put(1, "Transport_Layer_Cause");
        mapValues.put(2, "NAS_Cause");
        mapValues.put(3, "Protocol_Cause");
        mapValues.put(4, "Miscellaneous_Cause");
        mapValues.put(255, "Other");
        mappings.put("FIR_FAIL_CAUSE_TYPE|FIR_FAIL_CAUSE_TYPE", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "NativeGUTI");
//        mapValues.put(1, "MappedGUTI");
//        mappings.put("OLD_GUTI_TYPE|OLD_GUTI_TYPE", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(12, "E-RAB_Release_Indication");
//        mapValues.put(4, "UE_Context_Release_Request");
//        mappings.put("RLS_REQ_TYPE|ReleasedType", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(5, "UE_Context_Release");
//        mapValues.put(11, "E-RAB_Release");
//        mappings.put("RLS_TRANS_TYPE|RLS_TRANS_TYPE", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0,"Successful");
//        mapValues.put(1, "Failed");
//        mappings.put("RLS_TRANS_SUCCEED_FLAG|ReleaseStatus", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(0, "Radio_Network_Layer_Cause");
//        mapValues.put(1, "Transport_Layer_Cause");
//        mapValues.put(2, "NAS_Cause");
//        mapValues.put(3, "Protocol_Cause");
//        mapValues.put(4, "Miscellaneous_Cause");
//        mapValues.put(255, "Other");
//        mappings.put("RLS_TRANS_CAUSE_TYPE|RLS_TRANS_CAUSE_TYPE", mapValues);

//        mapValues = new HashMap<>();
//        mapValues.put(1000, "Timeout");
//        mappings.put("RLS_TRANS_CAUSE|RLS_TRANS_CAUSE", mapValues);
//
//        mapValues = new HashMap<>();
//        mapValues.put(1, "Initial_request");
//        mapValues.put(2, "Handover");
//        mapValues.put(3, "Unused");
//        mapValues.put(4, "Emergency");
//        mappings.put("REQUEST_TYPE|REQUEST_TYPE", mapValues);
        return mappings;
    }

    private Column convertUnixToTimestamp(Column column, boolean withMilliseconds) {
        return to_timestamp(from_unixtime(column), "yyyy-MM-dd HH:mm:ss" + (withMilliseconds? ".SSS" : ""));
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> mmeRef = references.get("mmeRef");

        //1-1 Mappings w references
//        Dataset<Row> causeCodeEMM = references.get("causeCodeEMMRef");
//        Dataset<Row> causeCodeESM = references.get("causeCodeESMRef");
//        Dataset<Row> causeCodeS1AP= references.get("causeCodeS1APRef");

//        Dataset<Row> mccRef= references.get("mccRef");
//        Dataset<Row> mncRef= references.get("mncRef");

        // Location Information
        locationRef = locationRef
                .select("CGISAI"
                        , "CELL_NAME", "SITE_NAME", "XPOS", "YPOS", "DIRECTIONANGLE", "LAYER4NAME", "LAYER3NAME"
                        , /*"LAYER2NAME", */"LAYER1NAME")
                .withColumnRenamed("CELL_NAME", "CellName")
                .withColumnRenamed("SITE_NAME", "SiteName")
                .withColumnRenamed("XPOS", "Cell_lat")
                .withColumnRenamed("YPOS", "Cell_lon")
                .withColumnRenamed("DIRECTIONANGLE", "CellAngle")
                .withColumnRenamed("LAYER4NAME", "CityName")
                .withColumnRenamed("LAYER3NAME", "ProvinceName")
//                .withColumnRenamed("LAYER2NAME", "CountryName")
                .withColumnRenamed("LAYER1NAME", "RegionName");
        locationRef.persist();

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"));
        deviceRef.persist();

        // MME
        mmeRef = mmeRef
                .select("MME_Name", "mme_sig_ip")
                .withColumnRenamed("mme_sig_ip", "mme_ref_sig_ip");
        mmeRef.persist();

        // Cause Code
//        causeCodeEMM.persist();
//        causeCodeESM.persist();
//        causeCodeS1AP.persist();

        //MCC / MNC
//        mccRef.persist();
//        mncRef.persist();

//        ds = ds.withColumn("POWEROFF_FLAG"
//                , when(col("EMM_TRANS_TYPE").equalTo(49) //Both the column name and the value was incorrect in doc
//                        , when(col("POWEROFF_FLAG").equalTo(0), "Normal detach")
//                                .when(col("POWEROFF_FLAG").equalTo(1), "Power switched off")
//                                .otherwise(col("POWEROFF_FLAG")))
//                        .otherwise(col("POWEROFF_FLAG")));

        ds = ds.withColumn("EMM_SUB_Transaction"
                        , when(col("EMM_TRANS_TYPE").equalTo(48)
                                , when(col("EMM_TRANS_SUB_TYPE").equalTo(1), "EPS_attach")
                                        .when(col("EMM_TRANS_SUB_TYPE").equalTo(2), "Combined_EPS-IMSI_attach")
                                        .when(col("EMM_TRANS_SUB_TYPE").equalTo(6), "EPS_emergency_attach")
                                        .otherwise(col("EMM_TRANS_SUB_TYPE")))
                                .when(col("EMM_TRANS_TYPE").equalTo(54)
                                        , when(col("EMM_TRANS_SUB_TYPE").equalTo(0), "TA_updating")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(1), "Combined_TA-LA_updating")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(2), "Combined_TA-LA_updating_with_IMSI_attach")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(3), "Periodic_updating")
                                                .otherwise(col("EMM_TRANS_SUB_TYPE")))
                                .when(col("EMM_TRANS_TYPE").equalTo(49).and(col("EMM_DIRECTION").equalTo("eNodeB-initiated")) //UE_TO_NW??
                                        , when(col("EMM_TRANS_SUB_TYPE").equalTo(1), "EPS_detach")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(2), "IMSI_detach")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(3), "Combined_EPS-IMSI"))
                                .when(col("EMM_TRANS_TYPE").equalTo(49).and(col("EMM_DIRECTION").equalTo("MME-initiated")) //NW_TO_UE??
                                        , when(col("EMM_TRANS_SUB_TYPE").equalTo(1), "Re_attach_required")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(2), "Re_attach_not_required")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(3), "IMSI_detach"))
                                .when(col("EMM_TRANS_TYPE").equalTo(58)
                                        , when(col("EMM_TRANS_SUB_TYPE").equalTo(0), "MO_CSFB_or_1xCSFB")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(1), "MT_CSFB_or_1xCSFB")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(2), "MO_CSFB_emergency_call_or_1xCSFB_emergency_call")
                                                .when(col("EMM_TRANS_SUB_TYPE").equalTo(8), "Packet_services_via_S1"))
                                .otherwise(col("EMM_TRANS_SUB_TYPE")))
                .drop("EMM_TRANS_SUB_TYPE");

        ds = ds.withColumn("RoamingType"
                , when(col("HOMEMCC").equalTo("432").and(col("HOMEMNC").equalTo("11")), "Local")
                        .when(col("HOMEMCC").equalTo("432").and(col("HOMEMNC").notEqual("11")), "National")
                        .when(col("HOMEMCC").notEqual("432").and(col("HOMEMNC").isNotNull()), "International")
                        .otherwise("unknown"));

        //Formations
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).equalTo(14),
                        concat(lit("0"), substring(col("IMEI"), 0, 7)))
                        .when(length(col("IMEI")).equalTo(13),
                                concat(lit("00"), substring(col("IMEI"), 0, 6)))
                        .otherwise(substring(col("IMEI"), 0, 8))
        );

        ds = ds.withColumn("RAI_TAI"
                        , concat(expr("substring(RAI_TAI,1,5)"), lit("_"), expr("conv(substring(RAI_TAI,-4),16,10)")))
//                .withColumn("OLD_RAI_TAI"
//                        , concat(expr("substring(OLD_RAI_TAI,1,5)"), lit("_"), expr("conv(substring(OLD_RAI_TAI,-4),16,10)")))
                .withColumn("CellID"
                        , concat(expr("substring(SAI_CGI_ECGI,1,5)"), lit("_")
                                , expr("conv(substring(SAI_CGI_ECGI,6,5),16,10)"), lit("_")
                                , expr("conv(substring(SAI_CGI_ECGI,-2),16,10)")))
//                .withColumn("OLD_SAI_CGI_ECGI"
//                        , concat(expr("substring(OLD_SAI_CGI_ECGI,1,5)"), lit("_")
//                                , expr("conv(substring(OLD_SAI_CGI_ECGI,6,5),16,10)"), lit("_")
//                                , expr("conv(substring(OLD_SAI_CGI_ECGI,-2),16,10)")))
                                ;

//        ds = ds.withColumn("StartTime",
//                convertUnixToTimestamp(col("PROC_STARTTIME").cast(DataTypes.DoubleType)
//                        .plus(col("PROC_STARTTIME_MSEC").divide(1000)), true));
//        ds = ds.withColumn("EndTime",
//                convertUnixToTimestamp(col("PROC_ENDTIME").cast(DataTypes.DoubleType)
//                        .plus(col("PROC_ENDTIME_MSEC").divide(1000)), true));
        ds = ds.withColumn("RECORD_DATE", convertUnixToTimestamp(col("PROC_STARTTIME"), false));
        ds = addTimeColumns(ds);

        HashMap<String, HashMap<Integer, String>> mappings = getOneToOneMappings();
        ds = applyOneToOneMappings(ds, mappings);

//        String[] cols = ds.columns();
//
//        for (String col : new String[]{
//                //location
//                "CellName", "SiteName", "Cell_lat", "Cell_lon", "CellAngle",
//                "CityName", "ProvinceName",/* "CountryName", */"RegionName",
//                //device
//                "DeviceBrand", "DeviceModel", "DeviceSupportedMode", "DeviceType", "DeviceOS",
//                //mme
//                "MME_Name"
//                //1-1 Mappings
//                /*, "Auth_Cause", "SecurityCause", "UE_ESM_Cause", "NW_ESM_Cause"*/
//        }) {
//            cols = Arrays.copyOf(cols, cols.length + 1);
//            cols[cols.length - 1] = col;
//        }
        String[] cols = new String[]{
                "RECORD_DATE", "RECORD_DATE_TEHRAN", "day",
                //location
                "CellName", "SiteName", "Cell_lat", "Cell_lon", "CellAngle",
                "CityName", "ProvinceName",/* "CountryName", */"RegionName",
                //device
                "DeviceBrand", "DeviceModel", "DeviceSupportedMode", "DeviceType", "DeviceOS",
                //mme
                "MME_Name",
                "partition", "offset",
                "SID", "IMSI", "MSISDN", "IMEI", "RAT", "RAI_TAI", "CellID", "APN", "ProcedureType",
                "PROC_STARTTIME", "EMM_Transaction", "EMM_SUB_Transaction", "EMM_REQ_TIME_SEC", "EMM_REQ_TIME_MSEC",
                "EMM_END_TIME_SEC", "EMM_END_TIME_MSEC", "EMM_Status", "UE_ESM_Transaction", "ESM_UE_REQ_TIME_SEC",
                "ESM_UE_REQ_TIME_MSEC", "ESM_UE_END_TIME_SEC", "ESM_UE_END_TIME_MSEC", "UE_ESM_Status",
                "NW_ESM_Transaction", "ESM_NW_REQ_TIME_SEC", "ESM_NW_REQ_TIME_MSEC", "ESM_NW_END_TIME_SEC",
                "ESM_NW_END_TIME_MSEC", "NW_ESM_Status", "PAGING_REQ_TIME_SEC", "PAGING_REQ_TIME_MSEC",
                "PAGING_RSP_TIME_SEC", "PAGING_RSP_TIME_MSEC", "Paging_Status", "S1AP_Transaction", "S1AP_REQ_TIME_SEC",
                "S1AP_REQ_TIME_MSEC", "S1AP_END_TIME_SEC", "S1AP_END_TIME_MSEC", "S1AP_Status", "FIR_FAIL_TRANS",
                "FIR_FAIL_CAUSE_TYPE", "FIR_FAIL_CAUSE", "RoamingType"
        };

//        ds = mapColumnUsingRef(ds, causeCodeEMM, col("EMM_CAUSE"), "ID", "Name"
//                , "EMM_CAUSE", col("EMM_CAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeEMM, col("IDENTITY_CAUSE"), "ID", "Name"
//                , "IDENTITY_CAUSE", col("IDENTITY_CAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeEMM, col("AUTH_REJ_EMM_CAUSE"), "ID", "Name"
//                , "Auth_Cause", col("AUTH_REJ_EMM_CAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeEMM, col("SECURITY_MODE_CAUSE"), "ID", "Name"
//                , "SecurityCause", col("SECURITY_MODE_CAUSE")); //FIX: I haven't dropped the prev column in these cases. is it ok?

//        ds = mapColumnUsingRef(ds, causeCodeESM, col("ESM_UE_REJ_CAUSE"), "ID", "Name"
//                , "UE_ESM_Cause", col("ESM_UE_REJ_CAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeESM, col("ESM_UE_ESMCAUSE"), "ID", "Name"
//                , "ESM_UE_ESMCAUSE", col("ESM_UE_ESMCAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeESM, col("ESM_NW_REJ_CAUSE"), "ID", "Name"
//                , "NW_ESM_Cause", col("ESM_NW_REJ_CAUSE"));
//        ds = mapColumnUsingRef(ds, causeCodeESM, col("ESM_NW_ESMCAUSE"), "ID", "Name"
//                , "ESM_NW_ESMCAUSE", col("ESM_NW_ESMCAUSE"));

//        ds = mapColumnUsingRef(ds, causeCodeS1AP
//                , col("S1AP_CAUSE_TYPE").multiply(10000).plus(col("S1AP_CAUSE"))
//                , "ID", "Name"
//                , "S1AP_CAUSE", col("S1AP_CAUSE"));

//        ds = mapColumnUsingRef(ds, mncRef, col("HOMEMCC").cast(DataTypes.IntegerType)
//                        .multiply(1000).plus(col("HOMEMNC").cast(DataTypes.IntegerType))
//                , "ID", "Name"
//                , "HOMEMNC", col("HOMEMNC"));
//        ds = mapColumnUsingRef(ds, mccRef, col("HOMEMCC"), "ID", "Name"
//                , "HOMEMCC", col("HOMEMCC"));

        Column locationCond = ds.col("SAI_CGI_ECGI").equalTo(col("loc_ref.CGISAI"));
        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));
        Column mmeCond = ds.col("MME_SIG_IP").equalTo(col("mme_ref.mme_ref_sig_ip"));

        ds = ds.join(broadcast(locationRef.as("loc_ref")), locationCond, "leftouter")
                .join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(mmeRef.as("mme_ref")), mmeCond, "leftouter")
//                .drop("mme_ref.mme_sig_ip")
                .withColumn("DeviceBrand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("DeviceModel", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("DeviceSupportedMode", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("DeviceType", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("DeviceOS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .selectExpr(cols);

        ds = ds.withColumnRenamed("MME_SIG_IP", "MME_IP_Address")
                .withColumnRenamed("RAN_NE_SIG_IP", "eNodeB_IP_Address");
        return ds;
    }

    @Override
    public Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
