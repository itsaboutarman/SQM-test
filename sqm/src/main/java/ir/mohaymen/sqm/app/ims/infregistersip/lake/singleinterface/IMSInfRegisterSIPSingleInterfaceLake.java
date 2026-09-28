package ir.mohaymen.sqm.app.ims.infregistersip.lake.singleinterface;

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
public class IMSInfRegisterSIPSingleInterfaceLake extends Lake {
    public IMSInfRegisterSIPSingleInterfaceLake(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return  ds.select("Start_Time_s", "Start_Time_ms", "End_Time_ms", "Access_Type",
                "User_Type", "Registration_Type", "Service_Status", "Interface_Type",
                "Source_NE_Type", "Source_NE_IP_Address", "Source_NE_Name" ,
                "Destination_NE_Name", "Destination_NE_Type", "Destination_NE_IP_Address" ,"IMEI",
                "Public_Identity_1_in_TEL_URI_Format",
                "Private_Identity_in_TEL_URI_Format", "Brand", "Model",
                "Device_External_IP_Address", "Device_Type", "SV", "Access_Location_Info", "CELL",
                "Country", "RegionName", "ProvinceName", "CityName", "First_Release_Time_s",
                "First_Release_NE_Type", "First_Release_NE_IP_Address","First_Release_NE_Name",
                "Release_Cause_Code", "Warning_Code", "Authentication_Request_Time_ms",
                "Authentication_Response_Time_ms", "Visited_Network_Domain", "Area_Code", "TAI",
                "Authentication_Mode", "Registration_Request_Valid_Duration_s" ,
                "Session_id" , "Association_id", "Probe_id", "IMS_Call_id", "IMS_Charging_id");
    }

    @Override
    protected Dataset<Row> manipulateData(Dataset<Row> ds) {
        Column tehranTime = from_utc_timestamp(col("Start_Time"), "Asia/Tehran");
        
        ds = ds.withColumn("Start_Time" ,to_timestamp(from_unixtime(col("Start_Time_s").plus(col("Start_Time_ms").divide(1000))), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("End_Time" , to_timestamp(from_unixtime(col("Start_Time_s").plus(col("Start_Time_ms").divide(1000)).plus(col("End_Time_ms").divide(1000))), "yyyy-MM-dd HH:mm:ss"));
                .withColumn("day", date_trunc("day", tehranTime))
                .withColumn("hour", hour(tehranTime))
                .withColumn("minute_15", minute(tehranTime).minus(minute(tehranTime).mod(15)));

        ds = ds.withColumnRenamed("Session_id","Session_ID")
        .withColumnRenamed("Association_id","Association_ID")
        .withColumnRenamed("Probe_id","Probe_ID")
        .withColumnRenamed("IMS_Call_id","IMS_Call_ID")
        .withColumnRenamed("IMS_Charging_id","IMS_Charging_ID");
        ds = ds.withColumnRenamed("Source_NE_IP_Address","Source_NE_IP")
                .withColumnRenamed("Destination_NE_IP_Address","Destination_NE_IP")
                .withColumnRenamed("Private_Identity_in_TEL_URI_Format","IMSI" )
                .withColumnRenamed("Public_Identity_1_in_TEL_URI_Format","MSISDN" )
                .withColumnRenamed("Brand", "Device_Brand")
                .withColumnRenamed("Model", "Device_Model")
                .withColumnRenamed("Device_External_IP_Address","Device_IP")
                .withColumnRenamed("Device_Type","Device_OS")
                .withColumnRenamed("SV","IMEI_SV")
                .withColumnRenamed("Country","Layer1_Name")
                .withColumnRenamed("RegionName","Layer2_Name")
                .withColumnRenamed("ProvinceName","Layer3_Name")
                .withColumnRenamed("CityName","Layer4_Name")
                .withColumnRenamed("First_Release_Time_s","First_Release_Time")
                .withColumnRenamed("Authentication_Request_Time_ms","401_Message_Delay_ms")
                .withColumnRenamed("First_Release_NE_Type","First_Release_NE_Type_source")
                .withColumnRenamed("Destination_NE_Type" ,"Destination_NE_Type_source")
                .withColumnRenamed("Source_NE_Type", "Source_NE_Type_source")
                .withColumnRenamed("Interface_Type","Interface_Type_source")
                .withColumnRenamed("Service_Status" ,"Service_Status_source")
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
        Dataset<Row> registrationType = references.get("registrationType")
                .withColumnRenamed("Key","registrationTypeKey")
                .withColumnRenamed("Value" ,"Service_Type");
        registrationType.persist();
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

        Dataset<Row> firstReleaseNEType = references.get("firstReleaseNEType")
                .withColumnRenamed("Key" ,"firstReleaseNETypeKey")
                .withColumnRenamed("Value" ,"First_Release_NE_Type");
        firstReleaseNEType.persist();
        Dataset<Row> SIP = references.get("SIP")
                .withColumnRenamed("Key" ,"SIPKey")
                .withColumnRenamed("Value" ,"SIP_Response_Code");
        SIP.persist();

        Dataset<Row> authType = references.get("authType")
                .withColumnRenamed("Key" , "authTypeKey")
                .withColumnRenamed("Value" ,"Authentication_Type");

        authType.persist();
        ds = ds.join(broadcast(accessType) , ds.col("Access_Type_source").equalTo(accessType.col("AccessTypeKey")) , "leftouter")
                .join(broadcast(userType) , ds.col("User_Type_source").equalTo(userType.col("userTypeKey")), "leftouter")
                .join(broadcast(registrationType) , ds.col("Registration_Type").equalTo(registrationType.col("registrationTypeKey")), "leftouter")
                .join(broadcast(serviceStatus) , ds.col("Service_Status_source").equalTo(serviceStatus.col("serviceStatusKey")), "leftouter")
                .join(broadcast(interfaceType) , ds.col("Interface_Type_source").equalTo(interfaceType.col("interfaceTypeKey")), "leftouter")
                .join(broadcast(sourceNEType) , ds.col("Source_NE_Type_source").equalTo(sourceNEType.col("sourceNETypeKey")), "leftouter")
                .join(broadcast(destinationNEType) , ds.col("Destination_NE_Type_source").equalTo(destinationNEType.col("destinationNETypeKey")), "leftouter")
                .join(broadcast(firstReleaseNEType) , ds.col("First_Release_NE_Type_source").equalTo(firstReleaseNEType.col("firstReleaseNETypeKey")), "leftouter")
                .join(broadcast(SIP) , ds.col("Release_Cause_Code").equalTo(SIP.col("SIPKey")), "leftouter")
                .join(broadcast(authType) , ds.col("Authentication_Mode").equalTo(authType.col("authTypeKey")), "leftouter");

        ds = ds.drop("Access_Type_source")
                .drop("User_Type_source")
                .drop("Registration_Type")
                .drop("Service_Status_source")
                .drop("Interface_Type_source")
                .drop("Source_NE_Type_source")
                .drop("Destination_NE_Type_source")
                .drop("First_Release_NE_Type_source")
                .drop("Release_Cause_Code")
                .drop("Authentication_Mode")
                .drop("End_Time_ms")
                .drop("AccessTypeKey")
                .drop("userTypeKey")
                .drop("registrationTypeKey")
                .drop("serviceStatusKey")
                .drop("interfaceTypeKey")
                .drop("sourceNETypeKey")
                .drop("destinationNETypeKey")
                .drop("firstReleaseNETypeKey")
                .drop("SIPKey")
                .drop("authTypeKey");

                
        ds = generateKeyHash(ds, new String[]{
                "Start_Time", "End_Time", "IMEI", "MSISDN", "IMSI"});

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
