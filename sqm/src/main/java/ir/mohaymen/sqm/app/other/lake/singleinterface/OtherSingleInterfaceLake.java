package ir.mohaymen.sqm.app.other.lake.singleinterface;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Lake;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.util.Map;
import static org.apache.spark.sql.functions.*;
import static java.util.stream.Collectors.joining;
import java.util.Arrays;
import java.util.List;

@Job
public class OtherSingleInterfaceLake extends Lake {
    public OtherSingleInterfaceLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select("BEGIN_TIME", "BEGIN_TIME_MSEL", "END_TIME", "END_TIME_MSEL", "Protocol_Category_Name",
                "Protocol_Name", "L7_CARRIER_PROT", "HOST", "INTERFACEID", "RAN_NE_USER_IP", "SGSN_USER_IP","SID" , "table_id",
                "GGSN_USER_IP", "TCP_CONN_STATES", "IMSI", "MSISDN", "IMEI", "Brand", "Model",
                "Technology", "Type", "OS", "RAT", "User_Agent", "FST_URI", "Country", "Region",
                "Province", "City", "VISIT_TYPE", "L4_TYPE", "SGSN", "SGSN_POOL", "GGSN", "SGW", "PGW",
                "Application_Name", "APN_Edit", "CELL_Name", "BSC_Name", "RNC_Name", "ENODEB_ID", "LAC", "SAC", "CI",
                "ROAMING_TYPE", "ROAM_DIRECTION", "MS_IP", "SERVER_IP", "MS_PORT", "SERVER_PORT", "SGSN_SIG_IP",
                "GGSN_SIG_IP", "MCC", "MNC", "RAC", "TAC", "ECI", "TRANS_TYPE", "SITE_Name", "L4_UL_THROUGHPUT",
                "L4_DW_THROUGHPUT", "NETWORK_UL_TRAFFIC", "NETWORK_DL_TRAFFIC", "L4_UL_PACKETS", "L4_DW_PACKETS",
                "TCP_STATES", "TCP_RTT", "TCP_UL_OUTOFSEQU", "TCP_DW_OUTOFSEQU",
                "TCP_CONN_2_FAILED_TIMES", "TCP_CONN_3_FAILED_TIMES", "TCP_RTT_STEP1", "TCP_UL_RETRANS_WITHPL",
                "TCP_DW_RETRANS_WITHPL", "TCP_UL_PACKAGES_WITHPL", "TCP_DW_PACKAGES_WITHPL", "GET_NUM",
                "GET_SUCCEED_NUM", "POST_NUM", "POST_SUCCEED_NUM", "MS_WIN_STAT_TOTAL_NUM", "MS_WIN_STAT_SMALL_NUM",
                "MS_ACK_TO_1STGET_DELAY", "SERVER_ACK_TO_1STDATA_DELAY", "AVG_UL_RTT", "AVG_DW_RTT", "UL_RTT_LONG_NUM",
                "DW_RTT_LONG_NUM", "UL_RTT_STAT_NUM", "DW_RTT_STAT_NUM", "USER_PROBE_UL_LOST_PKT",
                "SERVER_PROBE_UL_LOST_PKT", "SERVER_PROBE_DW_LOST_PKT", "USER_PROBE_DW_LOST_PKT",
                "L7_UL_GOODPUT_FULL_MSS", "DATATRANS_UL_DURATION", "L7_DW_GOODPUT_FULL_MSS", "DATATRANS_DW_DURATION",
                "SIGNALING_USE_FLAG", "DL_FLIGHT_TOTAL_SIZE", "DL_FLIGHT_TOTAL_NUM", "DL_MAX_FLIGHT_SIZE",
                "UL_FLIGHT_TOTAL_SIZE", "UL_FLIGHT_TOTAL_NUM", "UL_MAX_FLIGHT_SIZE",
                "DL_CONTINUOUS_RETRANSMISSION_DELAY", "USER_HUNGRY_DELAY", "SERVER_HUNGRY_DELAY", "FIRST_RAT",
                "SUB_PROT_TYPE", "TIMEZONE", "DST", "ARP", "ALLOWED_UL_MBR", "ALLOWED_DL_MBR",
                "TRAFFIC_CLASS_NEG", "THP_NEG", "QCI_NEG", "SERVICE_ENCRYPED_FLAG", "TOTAL_TCP_RTT",
                "TOTAL_TCP_RTT_STEP1", "APP_ID", "PROBEID", "Record_type", "L4_UL_GOODPUT", "L4_DW_GOODPUT", "SP",
                "UFDR_Type", "DL_TRANS_DELAY", "UL_TRANS_DELAY", "AVG_UL_RTT_MICRO_SEC", "AVG_DW_RTT_MICRO_SEC", "FLAG",
                "RECORD_DATE", "day", "RECORD_DATE_TEHRAN", "Latitude", "Longitude"
        );
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        ds = ds.withColumn("Start_Time", to_timestamp(from_unixtime(col("BEGIN_TIME").plus(col("BEGIN_TIME_MSEL").divide(1000))), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("End_Time", to_timestamp(from_unixtime(col("END_TIME").plus(col("END_TIME_MSEL").divide(1000))), "yyyy-MM-dd HH:mm:ss"));


        ds = ds.withColumnRenamed("Protocol_Category_Name", "Service")
                .withColumnRenamed("Protocol_Name", "Protocol")
                .withColumnRenamed("HOST", "Host_Name")
                .withColumnRenamed("RAN_NE_USER_IP", "RAN_NE_User_Plane_IP_Address")
                .withColumnRenamed("SGSN_USER_IP", "SGSN_SGW_User_Plane_IP")
                .withColumnRenamed("GGSN_USER_IP", "GGSN_PGW_User_Plane_IP")
                .withColumnRenamed("Brand", "Device_Brand")
                .withColumnRenamed("Model", "Device_Model")
                .withColumnRenamed("Technology", "Device_Technology")
                .withColumnRenamed("Type", "Device_Type")
                .withColumnRenamed("OS", "Device_OS")
                .withColumnRenamed("FST_URI", "First_URI_Visited")
                .withColumnRenamed("APN_Edit", "APN")
                .withColumnRenamed("ENODEB_ID", "ENODEB_Name")
                .withColumnRenamed("MS_IP", "MS_IP_Address")
                .withColumnRenamed("SERVER_IP", "Server_IP_Address")
                .withColumnRenamed("MS_PORT", "MS_Port")
                .withColumnRenamed("SERVER_PORT", "Server_Port")
                .withColumnRenamed("SGSN_SIG_IP", "SGSN_Signaling_Plane_IP_Address")
                .withColumnRenamed("GGSN_SIG_IP", "GGSN_Signaling_Plane_IP_Address")
                .withColumnRenamed("L4_UL_THROUGHPUT", "Uplink_Traffic")
                .withColumnRenamed("L4_DW_THROUGHPUT", "Downlink_Traffic")
                .withColumnRenamed("NETWORK_UL_TRAFFIC", "Network_Uplink_Traffic")
                .withColumnRenamed("NETWORK_DL_TRAFFIC", "Network_Downlink_Traffic")
                .withColumnRenamed("L4_UL_PACKETS", "Uplink_Packets")
                .withColumnRenamed("L4_DW_PACKETS", "Downlink_Packets")
                .withColumnRenamed("TCP_STATES", "TCP_Connection_Release_Status")
                .withColumnRenamed("TCP_RTT", "TCP_Connect_RTT_ms")
                .withColumnRenamed("TCP_UL_OUTOFSEQU", "TCP_Uplink_Out_of_Order_Packets")
                .withColumnRenamed("TCP_DW_OUTOFSEQU", "TCP_Downlink_Out_of_Order_Packets")
                .withColumnRenamed("TCP_CONN_2_FAILED_TIMES", "SYN_ACK_Failures_During_TCP_Handshakes")
                .withColumnRenamed("TCP_CONN_3_FAILED_TIMES", "ACK_Failures_During_TCP_Handshakes")
                .withColumnRenamed("TCP_RTT_STEP1", "TCP_RTT_of_Step_1_ms")
                .withColumnRenamed("TCP_UL_PACKAGES_WITHPL", "TCP_Uplink_Packets_with_Payload")
                .withColumnRenamed("TCP_DW_PACKAGES_WITHPL", "TCP_Downlink_Packets_with_Payload")
                .withColumnRenamed("GET_NUM", "GET_Transactions")
                .withColumnRenamed("GET_SUCCEED_NUM", "Successful_GET_Transactions")
                .withColumnRenamed("POST_NUM", "POST_Transactions")
                .withColumnRenamed("POST_SUCCEED_NUM", "Successful_POST_Transactions")
                .withColumnRenamed("MS_WIN_STAT_TOTAL_NUM", "Device_Window_Size_Calculations")
                .withColumnRenamed("MS_WIN_STAT_SMALL_NUM", "Device_Side_Small_Window_Times")
                .withColumnRenamed("MS_ACK_TO_1STGET_DELAY", "Delay_Between_First_TCP_and_First_GET_ms")
                .withColumnRenamed("SERVER_ACK_TO_1STDATA_DELAY", "Delay_Between_First_GET_ACK_and_First_Data_Packet_ms")
                .withColumnRenamed("AVG_UL_RTT", "Average_Uplink_RTT")
                .withColumnRenamed("AVG_DW_RTT", "Average_Downlink_RTT")
                .withColumnRenamed("UL_RTT_LONG_NUM", "Number_of_Timer_Average_Uplink_RTT_Is_Longer_Than_500_ms")
                .withColumnRenamed("DW_RTT_LONG_NUM", "Number_of_Timer_Average_Downlink_RTT_Is_Longer_Than_1s")
                .withColumnRenamed("UL_RTT_STAT_NUM", "Uplink_RTT_Calculations")
                .withColumnRenamed("DW_RTT_STAT_NUM", "Downlink_RTT_Calculations")
                .withColumnRenamed("USER_PROBE_UL_LOST_PKT", "User_Side_Uplink_Lost_Packets")
                .withColumnRenamed("SERVER_PROBE_UL_LOST_PKT", "Server_Side_Uplink_Lost_Packets_")
                .withColumnRenamed("SERVER_PROBE_DW_LOST_PKT", "Server_Side_Downlink_Lost_Packets")
                .withColumnRenamed("USER_PROBE_DW_LOST_PKT", "User_Side_Downlink_Lost_Packets")
                .withColumnRenamed("L7_UL_GOODPUT_FULL_MSS", "Uplink_Valid_Traffic")
                .withColumnRenamed("DATATRANS_UL_DURATION", "Uplink_Valid_Transmission_Duration")
                .withColumnRenamed("L7_DW_GOODPUT_FULL_MSS", "Downlink_Valid_Traffic")
                .withColumnRenamed("DATATRANS_DW_DURATION", "Downlink_Valid_Transmission_Duration")
                .withColumnRenamed("SIGNALING_USE_FLAG", "Signaling_Consumption_Flag_Signaling_Messages")
                .withColumnRenamed("DL_FLIGHT_TOTAL_SIZE", "Downlink_TCP_Send_Window_Average_Size")
                .withColumnRenamed("DL_FLIGHT_TOTAL_NUM", "Total_Downlink_TCP_Send_Window_Size_Calculations")
                .withColumnRenamed("DL_MAX_FLIGHT_SIZE", "Downlink_Maximum_TCP_Send_Window_Size")
                .withColumnRenamed("UL_FLIGHT_TOTAL_SIZE", "Total_Uplink_TCP_Send_Window_Average_Size")
                .withColumnRenamed("UL_FLIGHT_TOTAL_NUM", "Total_Uplink_TCP_Send_Window_Size_Calculations")
                .withColumnRenamed("UL_MAX_FLIGHT_SIZE", "Uplink_Maximum_TCP_Send_Window_Size")
                .withColumnRenamed("DL_CONTINUOUS_RETRANSMISSION_DELAY", "Downlink_Continuous_Retransmission_Delay")
                .withColumnRenamed("USER_HUNGRY_DELAY", "User_Side_Hungry_Delay")
                .withColumnRenamed("SERVER_HUNGRY_DELAY", "Server_Side_Hunger_Delay")
                .withColumnRenamed("SUB_PROT_TYPE", "Probe_Protocol")
                .withColumnRenamed("TIMEZONE", "Time_Zone")
                .withColumnRenamed("DST", "DST_Offset")
                .withColumnRenamed("ARP", "Negotiated_Allocation_Retention_Priority")
                .withColumnRenamed("ALLOWED_UL_MBR", "Allowed__Uplink_Maximum_Bit_Rate")
                .withColumnRenamed("ALLOWED_DL_MBR", "Allowed__Downlink_Maximum_Bit_Rate")
                .withColumnRenamed("TRAFFIC_CLASS_NEG", "Negotiated_Traffic_Class")
                .withColumnRenamed("THP_NEG", "Negotiated_Handling_Traffic_Priority")
                .withColumnRenamed("QCI_NEG", "Negotiated_QoS_Class_Identifier")
                .withColumnRenamed("TOTAL_TCP_RTT", "Total_TCP_Connect_RTT_ms")
                .withColumnRenamed("TOTAL_TCP_RTT_STEP1", "Total_TCP_RTT_of_Step_1_ms")
                .withColumnRenamed("PROBEID", "probe_ID")
                .withColumnRenamed("L4_UL_GOODPUT", "TCP_Uplink_Payload_Traffic")
                .withColumnRenamed("L4_DW_GOODPUT", "TCP_Downlink_Payload_Traffic")
                .withColumnRenamed("FLAG", "Flag")
                .withColumnRenamed("SID", "Session_ID")

                .withColumnRenamed("L7_CARRIER_PROT", "Layer_7_Bearer_Protocol_Source")
                .withColumnRenamed("TCP_CONN_STATES", "TCP_Connection_Status_Source")
                .withColumnRenamed("INTERFACEID", "Interface_Type_Source")
                .withColumnRenamed("VISIT_TYPE", "VISIT_TYPE_Source")
                .withColumnRenamed("L4_TYPE", "Transport_Protocol_Source")
                .withColumnRenamed("ROAMING_TYPE", "ROAMING_TYPE_Source")
                .withColumnRenamed("ROAM_DIRECTION", "Roaming_Direction_Source")
                .withColumnRenamed("TRANS_TYPE", "Transaction_Type_Source")
                .withColumnRenamed("SERVICE_ENCRYPED_FLAG", "Service_Encryped_Flag_Source");

        ds = ds.withColumn("Layer_7_Bearer_Protocol", when(col("Layer_7_Bearer_Protocol_Source").equalTo(0), "Non-HTTP/WAP protocol")
                        .when(col("Layer_7_Bearer_Protocol_Source").equalTo(1), "HTTP protocol")
                        .when(col("Layer_7_Bearer_Protocol_Source").equalTo(2), "WAP 1.X protocol")

        ).withColumn("TCP_Connection_Status", when(col("TCP_Connection_Status_Source").equalTo(0), "success")
                .when(col("TCP_Connection_Status_Source").equalTo(1), "fail")

        ).withColumn("VISIT_TYPE", when(col("VISIT_TYPE_Source").equalTo(3578), "APP")
                .when(col("VISIT_TYPE_Source").equalTo(1), "Unknown")
                .when(col("VISIT_TYPE_Source").equalTo(3665), "Unknown browser")
                .otherwise(col("VISIT_TYPE_Source").cast("string"))

        ).withColumn("Transport_Protocol", when(col("Transport_Protocol_Source").equalTo(1), "UDP")
                .when(col("Transport_Protocol_Source").equalTo(0), "TCP")

        ).withColumn("ROAMING_TYPE", when(col("ROAMING_TYPE_Source").equalTo(0), "Intra-province")
                .when(col("ROAMING_TYPE_Source").equalTo(1), "National")
                .when(col("ROAMING_TYPE_Source").equalTo(2), "International")
                .when(col("ROAMING_TYPE_Source").equalTo(3), "Local")

        ).withColumn("Roaming_Direction", when(col("Roaming_Direction_Source").equalTo(0), "Inbound")
                .when(col("Roaming_Direction_Source").equalTo(0), "Outbound")

        ).withColumn("Transaction_Type", when(col("Transaction_Type_Source").equalTo(0), "Send")
                .when(col("Transaction_Type_Source").equalTo(1), "Delivery")
                .when(col("Transaction_Type_Source").equalTo(2), "Read-Reply")
                .when(col("Transaction_Type_Source").equalTo(3), "Retrieve Dely")
                .when(col("Transaction_Type_Source").equalTo(4), "Retrieve ")
                .when(col("Transaction_Type_Source").equalTo(5), "Notification")
                .when(col("Transaction_Type_Source").equalTo(6), "WTP CONNECT")
                .when(col("Transaction_Type_Source").equalTo(7), "WTP Disconnect")

        ).withColumn("Service_Encryped_Flag", when(col("Service_Encryped_Flag_Source").equalTo(0), "non-Encrypted")
                .when(col("Service_Encryped_Flag_Source").equalTo(0), "Encrypted")
        );

        Dataset<Row> interfaceType = references.get("interfaceType")
                .withColumnRenamed("Key","InterfaceTypeKey")
                .withColumnRenamed("Value" ,"Interface_Type");
        interfaceType.persist();

        ds =  ds.join(broadcast(interfaceType) , ds.col("interface_Type_source").equalTo(interfaceType.col("InterfaceTypeKey")) , "leftouter");
        ds = ds.drop("InterfaceTypeKey");

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
