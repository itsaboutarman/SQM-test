package ir.mohaymen.sqm.app.voip.sourcing;
import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Arrays;
import java.util.Map;
import static org.apache.spark.sql.functions.*;
public class VoIPSourcing extends Sourcing {
    public VoIPSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> cell2gRef = getReference("location2gRef");
        Dataset<Row> cell3gRef = getReference("location3gRef");
        Dataset<Row> cell4gRef = getReference("location4gRef");
        Dataset<Row> deviceRef = getReference("deviceRef");
        Dataset<Row> ottRef = getReference("ottRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgwPgwRef = getReference("sgwPgwRef");
        //Dataset<Row> appRef = getReference("applicationRef");
        sgwPgwRef = sgwPgwRef.select("NE_Name","ip");
        Dataset<Row> sgwRef = sgwPgwRef.withColumnRenamed("NE_Name","SGW");
        Dataset<Row> pgwRef = sgwPgwRef.withColumnRenamed("NE_Name","PGW");

        sgsnRef = sgsnRef.select("NE_Name","Home_SGSN_Pool_Name" ,"User_Plane_IP_Address")
                .withColumnRenamed("NE_Name","SGSN")
                .withColumnRenamed("Home_SGSN_Pool_Name","SGSN_POOL");

        ggsnRef = ggsnRef.select("NE_Name","User_Plane_IP_Address")
                .withColumnRenamed("NE_Name","GGSN");

        deviceRef = deviceRef.select("TER_BRAND_NAME","TAC","TER_MODEL_NAME","TER_MODENAME",
                        "TER_TYPE_NAME_EN","TER_OS_TYPE_NAME")
                .withColumnRenamed("TAC","TAC_device");

        ottRef = ottRef.where(col("Sheet_Name").equalTo("VOIP"));
        Dataset<Row> protocolRef = ottRef.select("Protocol_ID" ,"Protocol_Name","Protocol_Category_Name");
        Dataset<Row> appRef = ottRef.select("Probe_Protocol_ID","Application_Name");

        cell2gRef =  cell2gRef.select("LAC","CID","City","Province","region","country","Cell_Name","BSC",
                        "SITE_Name","LAT","LNG")
                .withColumnRenamed("LAC","LAC_2g")
                .withColumn("LAC_2g_hex",hex(col("LAC_2g").cast(DataTypes.IntegerType)))
                .withColumn("CID_hex",hex(col("CID").cast(DataTypes.IntegerType)));

        cell3gRef = cell3gRef.select("LAC","SAC","City","Province","region","country","RNC",
                        "NODEB_LABEL","CELL_NAME","LAT","LNG")
                .withColumnRenamed("LAC","LAC_3g")
                .withColumnRenamed("SAC","SAC_3g")
                .withColumn("LAC_3g_hex",hex(col("LAC_3g").cast(DataTypes.IntegerType)))
                .withColumn("SAC_3g_hex",hex(col("SAC_3g").cast(DataTypes.IntegerType)));
        cell4gRef = cell4gRef.select("ECGI","City","Province","region","country","ENODEB_Name",
                "CELL_Name","LAT","LNG")
                .withColumn("ECGI_2" ,hex(split(col("ECGI") ,"-").getItem(2).cast(DataTypes.IntegerType)))
                .withColumn("ECGI_3",hex(split(col("ECGI") ,"-").getItem(3).cast(DataTypes.IntegerType)));
        protocolRef.persist();
        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();
        appRef.persist();
        cell2gRef.persist();
        cell3gRef.persist();
        cell4gRef.persist();
        deviceRef.persist();


        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        String[] cols = ds.columns();
        String[] searchCols = {"RECORD_DATE" , "RECORD_DATE_TEHRAN","IMEI" ,"IMSI" ,"MSISDN","RAT","partition","offset"};
        for (String col:searchCols){
            cols = ArrayUtil.removeFromArray(cols ,col);
        }
        String[] addedCols = {"new_City", "new_Province", "new_Region", "new_Country",
                "new_Cell_Name", "BSC_Name", "SITE_Name", "Latitude", "Longitude","RNC_Name","NodeB_Name","ENodeB_Name",
                "Brand","Model","Technology", "Type", "OS", "Protocol_Name","Protocol_Category_Name","new_SGSN",
                "new_SGSN_POOL", "new_GGSN", "new_SGW", "new_PGW", "new_Application"};
        String[] selectedColumns = ArrayUtil.add(searchCols ,ArrayUtil.add(addedCols,cols));
        ds = ds.withColumn("TAC_Padded" , lpad(col("IMEI"),15 ,"0").substr(0,8));

        Column cell4g_cond = ((ds.col("ECI").isNotNull()).and(ds.col("RAT").equalTo(6)).and(substring(ds.col("ECI"), 1, 5).equalTo(cell4gRef.col("ECGI_2") )).and(substring(ds.col("ECI"), -2, 2).equalTo(cell4gRef.col("ECGI_3"))));
        Column cell3g_cond = ((ds.col("RAT").equalTo(1)).and(ds.col("LAC").isNotNull()).and(ds.col("LAC").equalTo(cell3gRef.col("LAC_3g_hex"))).and(ds.col("SAC").equalTo(cell3gRef.col("SAC_3g_hex"))));
        Column cell2g_cond = (ds.col("RAT").equalTo(2).and(ds.col("LAC").equalTo(cell2gRef.col("LAC_2g_hex"))).and(ds.col("CI").equalTo(cell2gRef.col("CID_hex"))));
        Column deviceCond = (ds.col("TAC_Padded").equalTo(deviceRef.col("TAC_device")));
        Column protocol_cond = (ds.col("Protocol").equalTo(protocolRef.col("Protocol_ID")));
        Column sgsn_cond_rat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(sgsnRef.col("User_Plane_IP_Address"))));
        Column ggsn_cond_rat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef2.User_Plane_IP_Address"))));
        Column ggsn_cond_rat1 = ((ds.col("RAT").equalTo(lit(1))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef3.User_Plane_IP_Address"))));
        Column sgw_cond_rat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(col("sgwRef.ip"))));
        Column pgw_cond_rat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("pgwRef.ip"))));
        Column app_cond = (ds.col("APP_ID").equalTo(appRef.col("Probe_Protocol_ID")));

        ds = ds.join(broadcast(cell4gRef), cell4g_cond, "leftouter")
                .join(broadcast(cell3gRef), cell3g_cond, "leftouter")
                .join(broadcast(cell2gRef), cell2g_cond, "leftouter")
                .join(broadcast(protocolRef), protocol_cond, "leftouter")
                .join(broadcast(sgsnRef), sgsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsn_cond_rat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgw_cond_rat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgw_cond_rat6, "leftouter")
                .join(broadcast(appRef), app_cond, "leftouter")
                .join(broadcast(deviceRef) ,deviceCond , "leftouter")
                .withColumn("new_City",coalesce(cell2gRef.col("City"),cell3gRef.col("City"),cell4gRef.col("City")))
                .withColumn("new_Province", coalesce(cell2gRef.col("Province"),cell3gRef.col("Province"),cell4gRef.col("Province")))
                .withColumn("new_Region",coalesce(cell2gRef.col("region"),cell3gRef.col("region"),cell4gRef.col("region")))
                .withColumn("new_Country",coalesce(cell2gRef.col("country"),cell3gRef.col("country"),cell4gRef.col("country")))
                .withColumn("new_Cell_Name",coalesce(cell2gRef.col("Cell_Name"),cell3gRef.col("Cell_Name"),cell4gRef.col("Cell_Name")))
                .withColumn("BSC_Name",cell2gRef.col("BSC"))
                .withColumn("SITE_Name",cell2gRef.col("SITE_Name"))
                .withColumn("Latitude",coalesce(cell2gRef.col("LAT"),cell3gRef.col("LAT"),cell4gRef.col("LAT")))
                .withColumn("Longitude",coalesce(cell2gRef.col("LNG"),cell3gRef.col("LNG"),cell4gRef.col("LNG")))
                .withColumn("RNC_Name",cell3gRef.col("RNC"))
                .withColumn("NodeB_Name",cell3gRef.col("NODEB_LABEL"))
                .withColumn("ENodeB_Name",cell4gRef.col("ENODEB_Name"))
                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME") , lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME"), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("Protocol_Name" , coalesce(protocolRef.col("Protocol_Name") , lit("Unknown")))
                .withColumn("Protocol_Category_Name" , coalesce(protocolRef.col("Protocol_Category_Name") , lit("Unknown")))
                .withColumn("new_SGSN" ,sgsnRef.col("SGSN"))
                .withColumn("new_SGSN_POOL" ,sgsnRef.col("SGSN_POOL"))
                .withColumn("new_GGSN" , coalesce(col("ggsnRef2.GGSN"),col("ggsnRef3.GGSN")))
                .withColumn("new_SGW" ,col("sgwRef.SGW"))
                .withColumn("new_PGW" ,col("pgwRef.PGW"))
                .withColumn("new_Application" ,coalesce(appRef.col("Application_Name") , lit("Unknown")))
                .selectExpr(selectedColumns)
                .withColumnRenamed("new_City","City")
                .withColumnRenamed("new_Province","Province")
                .withColumnRenamed("new_Region","Region")
                .withColumnRenamed("new_Country","Country")
                .withColumnRenamed("new_Cell_Name","Cell_Name")
                .withColumnRenamed("new_SGSN","SGSN")
                .withColumnRenamed("new_SGSN_POOL","SGSN_POOL")
                .withColumnRenamed("new_GGSN","GGSN")
                .withColumnRenamed("new_SGW","SGW")
                .withColumnRenamed("new_PGW","PGW")
                .withColumnRenamed("new_Application","Application");
        return ds;
    }
    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
