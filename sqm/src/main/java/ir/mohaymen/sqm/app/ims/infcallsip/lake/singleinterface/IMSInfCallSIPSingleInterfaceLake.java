package ir.mohaymen.sqm.app.ims.infcallsip.lake.singleinterface;

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
public class IMSInfCallSIPSingleInterfaceLake extends Lake {
    public IMSInfCallSIPSingleInterfaceLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select("Start_Time_s", "Start_Time_ms", "End_Time_ms", "Access_Type",
                "User_Type", "Service_Type", "Service_Status", "Interface_Type",
                "Source_NE_Type", "Source_NE_IP_Address", "Source_NE_Name" ,
                "Destination_NE_Type", "Destination_NE_IP_Address", "Destination_NE_Name",
                "Calling_or_Called_Party_Indicator", 
                "Private_Identity_in_TEL_URI_Format", "Public_Identity_in_TEL_URI_Format", "IMEI",
                "Brand", "Model", "OS", "SV", 
                "Access_Location_Info", "CELL", "Country", "RegionName", "ProvinceName", "CityName",
                "Call_Ringing_Time", "Call_Answer_Time", "Call_Release_Time", "Call_Duration_s",
                "Audio_Codec", "Video_SDP_Codec", "Session_Interruption_Flag", 
                "Calling_Address_in_TEL_URI_Format", "Calling_Party_Identity",
                "Called_Party_Address_in_TEL_URI_Format", "Called_Party_Identity",
                "Dialing_Number_in_TEL_URI_Format", "Peer_Access_Type", "Peer_Access_Location_Info",
                "First_Release_NE_Name", "Release_Cause_Code", "Warning_Code",
                "Reason_Protocol", "Reason_Code",
                "Abnormal_Play_Tone_Protocol", "Abnormal_Play_Tone_Cause_Code",
                "ReTransMsgType", "RetransMsgTimeOffset_ms", "CS_Retry_Triggering_Message",
                "Time_Difference_Between_the_INVITE_and_480_ResponseCancel_Messages",
                "183_Delay", "PRACK_Delay", "PRACK_200_OK_Delay",
                "UPDATE_Delay_for_Precondition", "UPDATE_200_OK_Delay_for_Precondition", 
                "UPDATE_Delay_for_Early_Media", "UPDATE_200_OK_Delay",
                "UE_Handover_Capability_Flag", "IWF_Handover_Capability_Flag",
                "Visited_Network_Domain", "Area_Code", "TAI", "Redirected_Times",
                "Call_Forwarding_Reason", "Call_Redirection_Number", "Original_Called_Party_Address", 
                "Conference_URI", "User_Type_in_Conference", "call_hold",
                "Calling_Audio_SDP_IP_Address", "Calling_Audio_SDP_Port",
                "Calling_Video_SDP_IP_Address", "Calling_Video_SDP_Port",
                "Called_Audio_SDP1_IP_Address", "Called_Audio_SDP1_Port",
                "Called_Video_SDP_IP_Address", "Called_Video_SDP_Port",
                "Call_Waiting_Flag", "Video_RBT_Capacity", "UPDATE_200_OK_Result_for_RBT", "180_Early_Media_Type",
                "Called_Party_Address_in_SIP_URI_Format", "Calling_Address_in_SIP_URI_Format",
                "Device_External_IP_Address", "Device_External_Port", 
                "Device_Internal_IP_Address", "Device_Internal_Port",
                "Dialing_Address_in_SIP_URI_Format", 
                "First_Failure_NE_IP_Address", "First_Failure_NE_Type", "First_Failure_Time",
                "Home_Network_Domain", "Private_Identity_in_SIP_URI_Format", "Public_Identity_in_SIP_URI_Format",
                "Q_850_Failure_Cause_Code", "TEL_URI_Called_Party_Address_Type",
                "TEL_URI_Calling_Address_Type", "TEL_URI_Dialing_Number_Type", "Session_id"
                );     
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.withColumn("Start_Time" ,to_timestamp(from_unixtime(col("Start_Time_s").plus(col("Start_Time_ms").divide(1000))), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("End_Time" , to_timestamp(from_unixtime(col("Start_Time_s").plus(col("Start_Time_ms").divide(1000)).plus(col("End_Time_ms").divide(1000))), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("Peer_Access_Location_Info_Name", lit(null).cast(DataTypes.StringType))
                .withColumn("Reason_Text", lit(null).cast(DataTypes.StringType))
                .withColumn("P_Early_Media_Type_in_183_or_Update", lit(null).cast(DataTypes.StringType))
                .withColumn("Original_Called_Party_IMSI", lit(null).cast(DataTypes.StringType));

        ds = ds.withColumnRenamed("Session_id","Session_ID")
                .withColumnRenamed("Source_NE_IP_Address", "Source_NE_IP")
                .withColumnRenamed("Destination_NE_IP_Address", "Destination_NE_IP")
                .withColumnRenamed("Private_Identity_in_TEL_URI_Format", "IMSI" )
                .withColumnRenamed("Public_Identity_in_TEL_URI_Format", "MSISDN" )
                .withColumnRenamed("Brand", "Device_Brand")
                .withColumnRenamed("Model", "Device_Model")
                .withColumnRenamed("Device_Internal_IP_Address", "Device_IP")
                .withColumnRenamed("OS", "Device_OS")
                .withColumnRenamed("SV", "IMEI_SV")
                .withColumnRenamed("CELL", "Access_Location_Info_Name")
                .withColumnRenamed("Country", "Layer1_Name")
                .withColumnRenamed("RegionName", "Layer2_Name")
                .withColumnRenamed("ProvinceName", "Layer3_Name")
                .withColumnRenamed("CityName", "Layer4_Name")
                .withColumnRenamed("Call_Ringing_Time", "Alerting_Time_ms")
                .withColumnRenamed("Call_Answer_Time", "Answer_Time_ms")
                .withColumnRenamed("Call_Release_Time", "Release_Time_ms")
                .withColumnRenamed("Session_Interruption_Flag", "Session_Terminal_Flag")
                .withColumnRenamed("Calling_Address_in_TEL_URI_Format", "Calling_Party_Address")
                .withColumnRenamed("Called_Party_Address_in_TEL_URI_Format", "Called_Party_Address")
                .withColumnRenamed("Dialing_Number_in_TEL_URI_Format", "Dialing_Number")
                .withColumnRenamed("Warning_Code", "Warning_Text")
                .withColumnRenamed("ReTransMsgType", "Retrans_Message_Type")
                .withColumnRenamed("RetransMsgTimeOffset_ms", "Retrans_Message_Time_ms")
                .withColumnRenamed("Time_Difference_Between_the_INVITE_and_480_ResponseCancel_Messages", "CS_Retry_Triggering_Delay_ms")
                .withColumnRenamed("183_Delay", "SIP_183_Delay_ms")
                .withColumnRenamed("PRACK_Delay", "PRACK_Delay_ms")
                .withColumnRenamed("PRACK_200_OK_Delay", "PRACK_200_OK_Delay_ms")
                .withColumnRenamed("UPDATE_Delay_for_Precondition", "UPDATE_Delay_for_Precondition_ms")
                .withColumnRenamed("UPDATE_200_OK_Delay_for_Precondition", "UPDATE_200_OK_Delay_for_Precondition_ms")
                .withColumnRenamed("UPDATE_Delay_for_Early_Media", "UPDATE_Delay_for_Early_Media_ms")
                .withColumnRenamed("UPDATE_200_OK_Delay", "UPDATE_200_OK_Delay_for_Early_Media_ms")
                .withColumnRenamed("IWF_Handover_Capability_Flag", "IWF_SRVCC_Capability_Flag")
                .withColumnRenamed("Call_Redirection_Number", "Call_Forward_Address")
                .withColumnRenamed("User_Type_in_Conference", "Conference_User_Type")
                .withColumnRenamed("Calling_Audio_SDP_IP_Address", "Calling_Audio_SDP_IP")
                .withColumnRenamed("Calling_Video_SDP_IP_Address", "Calling_Video_SDP_IP")
                .withColumnRenamed("Called_Audio_SDP1_IP_Address", "Called_Audio_SDP_IP")
                .withColumnRenamed("Called_Audio_SDP1_Port", "Called_Audio_SDP_Port")
                .withColumnRenamed("Called_Video_SDP_IP_Address", "Called_Video_SDP_IP")
                .withColumnRenamed("180_Early_Media_Type", "P_Early_Media_Type_in_180_Message")
                
                .withColumnRenamed("First_Failure_NE_Type", "First_Failure_NE_Type_source")
                .withColumnRenamed("UPDATE_200_OK_Result_for_RBT", "Update_200_OK_Result_for_RBT_source")
                .withColumnRenamed("Video_RBT_Capacity", "Calling_UE_Video_RBT_Capacity_source")
                .withColumnRenamed("Call_Waiting_Flag", "Call_Waiting_Flag_source")
                .withColumnRenamed("call_hold", "Call_Hold_Flag_source")
                .withColumnRenamed("UE_Handover_Capability_Flag", "UE_SRVCC_Capability_Flag_source")
                .withColumnRenamed("CS_Retry_Triggering_Message", "CS_Retry_Triggering_Message_source")
                .withColumnRenamed("Abnormal_Play_Tone_Protocol", "Abnormal_Play_Tone_Protocol_source")
                .withColumnRenamed("Reason_Protocol", "Reason_Protocol_source")
                .withColumnRenamed("Release_Cause_Code", "SIP_Response_Code_source")
                .withColumnRenamed("Peer_Access_Type", "Peer_Access_Type_source")
                .withColumnRenamed("Video_SDP_Codec", "Video_Codec_source")
                .withColumnRenamed("Audio_Codec", "Audio_Codec_source")
                .withColumnRenamed("Calling_or_Called_Party_Indicator" ,"Call_Side_Type_source")
                .withColumnRenamed("Destination_NE_Type" ,"Destination_NE_Type_source")
                .withColumnRenamed("Source_NE_Type", "Source_NE_Type_source")
                .withColumnRenamed("Interface_Type","Interface_Type_source")
                .withColumnRenamed("Service_Status" ,"Service_Status_source")
                .withColumnRenamed("Service_Type" ,"Service_Type_source")
                .withColumnRenamed("User_Type" ,"User_Type_source")
                .withColumnRenamed("Access_Type" ,"Access_Type_source");


        Dataset<Row> accessType = references.get("accessType")
                .withColumnRenamed("Key","accessTypeKey")
                .withColumnRenamed("Value" ,"Access_Type");
        accessType.persist();
        Dataset<Row> userType = references.get("userType")
                .withColumnRenamed("Key","userTypeKey")
                .withColumnRenamed("Value" ,"User_Type");
        userType.persist();
        Dataset<Row> serviceType = references.get("serviceType")
                .withColumnRenamed("Key","serviceTypeKey")
                .withColumnRenamed("Value" ,"Service_Type");
        serviceType.persist();
        Dataset<Row> serviceStatus = references.get("serviceStatus")
                .withColumnRenamed("Key","serviceStatusKey")
                .withColumnRenamed("Value" ,"Service_Status");
        serviceStatus.persist();
        Dataset<Row> interfaceType = references.get("interfaceType")
                .withColumnRenamed("Key","interfaceTypeKey")
                .withColumnRenamed("Value" ,"Interface_Type");
        interfaceType.persist();
        Dataset<Row> sourceNEType = references.get("sourceNEType")
                .withColumnRenamed("Key","sourceNETypeKey")
                .withColumnRenamed("Value" ,"Source_NE_Type");
        sourceNEType.persist();
        Dataset<Row> destinationNEType = references.get("destinationNEType")
                .withColumnRenamed("Key","destinationNETypeKey")
                .withColumnRenamed("Value","Destination_NE_Type");
        destinationNEType.persist();
        Dataset<Row> callSideType = references.get("callSideType")
                .withColumnRenamed("Key","callSideTypeKey")
                .withColumnRenamed("Value","Call_Side_Type");
        callSideType.persist();
        Dataset<Row> audioCodec = references.get("audioCodec")
                .withColumnRenamed("Key","audioCodecKey")
                .withColumnRenamed("Value","Audio_Codec");
        audioCodec.persist();
        Dataset<Row> videoCodec = references.get("videoCodec")
                .withColumnRenamed("Key","videoCodecKey")
                .withColumnRenamed("Value","Video_Codec");
        videoCodec.persist();
        Dataset<Row> peerAccessType = references.get("peerAccessType")
                .withColumnRenamed("Key","peerAccessTypeKey")
                .withColumnRenamed("Value","Peer_Access_Type");
        peerAccessType.persist();
        Dataset<Row> SIP = references.get("SIP")
                .withColumnRenamed("Key" ,"SIPKey")
                .withColumnRenamed("Value" ,"SIP_Response_Code");
        SIP.persist();
        Dataset<Row> reasonProtocol = references.get("reasonProtocol")
                .withColumnRenamed("Key","reasonProtocolKey")
                .withColumnRenamed("Value","Reason_Protocol");
        reasonProtocol.persist();
        Dataset<Row> abnormalPlayToneProtocol = references.get("abnormalPlayToneProtocol")
                .withColumnRenamed("Key","abnormalPlayToneProtocolKey")
                .withColumnRenamed("Value","Abnormal_Play_Tone_Protocol");
        abnormalPlayToneProtocol.persist();
        Dataset<Row> csRetryTriggeringMessage = references.get("csRetryTriggeringMessage")
                .withColumnRenamed("Key","csRetryTriggeringMessageKey")
                .withColumnRenamed("Value","CS_Retry_Triggering_Message");
        csRetryTriggeringMessage.persist();
        Dataset<Row> ueSRVCCCapabilityFlag = references.get("ueSRVCCCapabilityFlag")
                .withColumnRenamed("Key","ueSRVCCCapabilityFlagKey")
                .withColumnRenamed("Value","UE_SRVCC_Capability_Flag");
        ueSRVCCCapabilityFlag.persist();
        Dataset<Row> callHoldFlag = references.get("callHoldFlag")
                .withColumnRenamed("Key","callHoldFlagKey")
                .withColumnRenamed("Value","Call_Hold_Flag");
        callHoldFlag.persist();
        Dataset<Row> callWaitingFlag = references.get("callWaitingFlag")
                .withColumnRenamed("Key","callWaitingFlagKey")
                .withColumnRenamed("Value","Call_Waiting_Flag");
        callWaitingFlag.persist();
        Dataset<Row> callingUEVideoRBTCapacity = references.get("callingUEVideoRBTCapacity")
                .withColumnRenamed("Key","callingUEVideoRBTCapacityKey")
                .withColumnRenamed("Value","Calling_UE_Video_RBT_Capacity");
        callingUEVideoRBTCapacity.persist();
        Dataset<Row> update200OKResultForRBT = references.get("update200OKResultForRBT")
                .withColumnRenamed("Key","update200OKResultForRBTKey")
                .withColumnRenamed("Value","Update_200_OK_Result_for_RBT");
        update200OKResultForRBT.persist();
        Dataset<Row> firstFailureNEType = references.get("firstFailureNEType")
                .withColumnRenamed("Key","firstFailureNETypeKey")
                .withColumnRenamed("Value","First_Failure_NE_Type");
        firstFailureNEType.persist();       

        ds =  ds.join(broadcast(accessType) , ds.col("Access_Type_source").equalTo(accessType.col("AccessTypeKey")) , "leftouter")
                .join(broadcast(userType) , ds.col("User_Type_source").equalTo(userType.col("userTypeKey")), "leftouter")
                .join(broadcast(serviceType) , ds.col("Service_Type_source").equalTo(serviceType.col("serviceTypeKey")), "leftouter")
                .join(broadcast(serviceStatus) , ds.col("Service_Status_source").equalTo(serviceStatus.col("serviceStatusKey")), "leftouter")
                .join(broadcast(interfaceType) , ds.col("Interface_Type_source").equalTo(interfaceType.col("interfaceTypeKey")), "leftouter")
                .join(broadcast(sourceNEType) , ds.col("Source_NE_Type_source").equalTo(sourceNEType.col("sourceNETypeKey")), "leftouter")
                .join(broadcast(destinationNEType) , ds.col("Destination_NE_Type_source").equalTo(destinationNEType.col("destinationNETypeKey")), "leftouter")
                .join(broadcast(callSideType) , ds.col("Call_Side_Type_source").equalTo(callSideType.col("callSideTypeKey")), "leftouter")
                .join(broadcast(audioCodec) , ds.col("Audio_Codec_source").equalTo(audioCodec.col("audioCodecKey")), "leftouter")
                .join(broadcast(videoCodec) , ds.col("Video_Codec_source").equalTo(videoCodec.col("videoCodecKey")), "leftouter")
                .join(broadcast(peerAccessType) , ds.col("Peer_Access_Type_source").equalTo(peerAccessType.col("peerAccessTypeKey")), "leftouter")
                .join(broadcast(SIP) , ds.col("SIP_Response_Code_source").equalTo(SIP.col("SIPKey")), "leftouter")
                .join(broadcast(reasonProtocol) , ds.col("Reason_Protocol_source").equalTo(reasonProtocol.col("reasonProtocolKey")), "leftouter")
                .join(broadcast(abnormalPlayToneProtocol) , ds.col("Abnormal_Play_Tone_Protocol_source").equalTo(abnormalPlayToneProtocol.col("abnormalPlayToneProtocolKey")), "leftouter")
                .join(broadcast(csRetryTriggeringMessage) , ds.col("CS_Retry_Triggering_Message_source").equalTo(csRetryTriggeringMessage.col("csRetryTriggeringMessageKey")), "leftouter")
                .join(broadcast(ueSRVCCCapabilityFlag) , ds.col("UE_SRVCC_Capability_Flag_source").equalTo(ueSRVCCCapabilityFlag.col("ueSRVCCCapabilityFlagKey")), "leftouter")
                .join(broadcast(callHoldFlag) , ds.col("Call_Hold_Flag_source").equalTo(callHoldFlag.col("callHoldFlagKey")), "leftouter")
                .join(broadcast(callWaitingFlag) , ds.col("Call_Waiting_Flag_source").equalTo(callWaitingFlag.col("callWaitingFlagKey")), "leftouter")
                .join(broadcast(callingUEVideoRBTCapacity) , ds.col("Calling_UE_Video_RBT_Capacity_source").equalTo(callingUEVideoRBTCapacity.col("callingUEVideoRBTCapacityKey")), "leftouter")
                .join(broadcast(update200OKResultForRBT) , ds.col("Update_200_OK_Result_for_RBT_source").equalTo(update200OKResultForRBT.col("update200OKResultForRBTKey")), "leftouter")
                .join(broadcast(firstFailureNEType) , ds.col("First_Failure_NE_Type_source").equalTo(firstFailureNEType.col("firstFailureNETypeKey")), "leftouter");

        ds = ds.drop("Access_Type_source")
                .drop("User_Type_source")
                .drop("Service_Type_source")
                .drop("Service_Status_source")
                .drop("Interface_Type_source")
                .drop("Source_NE_Type_source")
                .drop("Destination_NE_Type_source")
                .drop("Call_Side_Type_source")
                .drop("Audio_Codec_source")
                .drop("Video_Codec_source")
                .drop("Peer_Access_Type_source")
                .drop("SIP_Response_Code_source")
                .drop("Reason_Protocol_source")
                .drop("Abnormal_Play_Tone_Protocol_source")
                .drop("CS_Retry_Triggering_Message_source")
                .drop("UE_SRVCC_Capability_Flag_source")
                .drop("Call_Hold_Flag_source")
                .drop("Call_Waiting_Flag_source")
                .drop("Calling_UE_Video_RBT_Capacity_source")
                .drop("Update_200_OK_Result_for_RBT_source")
                .drop("First_Failure_NE_Type_source")
                .drop("AccessTypeKey")
                .drop("userTypeKey")
                .drop("serviceTypeKey")
                .drop("serviceStatusKey")
                .drop("interfaceTypeKey")
                .drop("sourceNETypeKey")
                .drop("destinationNETypeKey")
                .drop("callSideTypeKey")
                .drop("audioCodecKey")
                .drop("videoCodecKey")
                .drop("peerAccessTypeKey")
                .drop("SIPKey")
                .drop("reasonProtocolKey")
                .drop("abnormalPlayToneProtocolKey")
                .drop("csRetryTriggeringMessageKey")
                .drop("ueSRVCCCapabilityFlagKey")
                .drop("callHoldFlagKey")
                .drop("callWaitingFlagKey")
                .drop("callingUEVideoRBTCapacityKey")
                .drop("update200OKResultForRBTKey")
                .drop("firstFailureNETypeKey");

                
        // ds = generateKeyHash(ds, new String[]{"Session_ID"});
        ds.printSchema();
        return ds;
    }
    
    protected Dataset<Row> generateKeyHash(Dataset<Row> ds, String[] cols) {
        List<String> columns = Arrays.asList(cols);
        String nvlExpr = columns.stream().map(
                        i -> "nvl(cast ("+i+" as string),'')"
                ).collect(joining(", ", "concat_ws('-'," , ")"));

        return ds.withColumn("key_hash",xxhash64(expr(nvlExpr)));
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds;
    }
}
