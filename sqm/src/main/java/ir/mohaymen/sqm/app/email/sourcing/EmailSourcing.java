package ir.mohaymen.sqm.app.email.sourcing;
import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.hadoop.shaded.org.eclipse.jetty.util.ArrayUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;
import java.util.Map;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
public class EmailSourcing extends Sourcing {
    public EmailSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        Dataset<Row> location2gRef = references.get("location2gRef");
        Dataset<Row> location3gRef = references.get("location3gRef");
        Dataset<Row> location4gRef = references.get("location4gRef");
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> protocolRef = references.get("protocolRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");
//        Dataset<Row> failureRef = getReference("failureRef");

        //Failure Cause
//        failureRef = failureRef
//                .withColumnRenamed("Failure_Cause_Description" , "Failure_Cause_Desc");
//        failureRef.persist();

        // NE Information
        sgsnRef = sgsnRef.select("NE_Name","Home_SGSN_Pool_Name" ,"User_Plane_IP_Address")
                .withColumnRenamed("NE_Name","SGSN")
                .withColumnRenamed("Home_SGSN_Pool_Name","SGSN_POOL");


        ggsnRef = ggsnRef.select("NE_Name","User_Plane_IP_Address")
                .withColumnRenamed("NE_Name","GGSN");

        Dataset<Row> sgwRef = sgw_pgw_ref.select("NE_Name","ip")
                .withColumnRenamed("NE_Name", "SGW");

        Dataset<Row> pgwRef = sgw_pgw_ref.select("NE_Name","ip")
                .withColumnRenamed("NE_Name", "PGW");


        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();

        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();


        location2gRef =  location2gRef.select("LAC","CID","City","Province","region","country","Cell_Name","BSC",
                        "SITE_Name","LAT","LNG");
        location2gRef = location2gRef
                .withColumnRenamed("LAC","LAC2g")
                .withColumnRenamed("CID","CID2g")
                .withColumn("LAC2g_hex" , hex(col("LAC2g").cast(DataTypes.IntegerType)))
                .withColumn("CID_hex" , hex(col("CID2g").cast(DataTypes.IntegerType)));
        location2gRef.persist();

        location3gRef = location3gRef.select("LAC","SAC","City","Province","region","country","RNC",
                        "NODEB_LABEL","CELL_NAME","LAT","LNG")
                .withColumnRenamed("LAC" ,"LAC3g");

        location3gRef = location3gRef.withColumn("LAC3g_hex" ,
                        hex(location3gRef.col("LAC3g").cast(DataTypes.IntegerType)))
                .withColumn("SAC3g_hex" , hex(location3gRef.col("SAC").cast(DataTypes.IntegerType)))
                .drop("SAC");
        location3gRef.persist();

        location4gRef = location4gRef.select("ECGI","City","Province","region","country","ENODEB_Name",
                "CELL_Name","LAT","LNG");


        location4gRef = location4gRef.withColumn("ECGI3_hex" , hex(split(location4gRef.col("ECGI"),"-").getItem(2).cast(DataTypes.IntegerType)))
                .withColumn("ECGI4_hex" , hex(split(location4gRef.col("ECGI"),"-").getItem(3).cast(DataTypes.IntegerType)));
        location4gRef.persist();



       protocolRef =  protocolRef.select("Protocol_ID" ,"Protocol_Name","Protocol_Category_Name");

      protocolRef.persist();
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("BEGIN_TIME")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = ds.withColumn("day", window(col("RECORD_DATE"), "1 day").getField("start"));
        ds = ds.withColumn("RECORD_DATE_TEHRAN", col("RECORD_DATE").plus(expr("INTERVAL 3 HOURS 30 MINUTES")));


        String[] cols = ds.columns();
        String[] addedCols = {"new_City", "new_Province", "new_Region", "new_Country",
                "new_Cell_Name", "BSC_Name", "SITE_Name", "Latitude", "Longitude","RNC_Name","NodeB_Name","ENodeB_Name",
                "Brand","Model","Technology", "Type", "OS", "Protocol_Name","Protocol_Category_Name","new_SGSN",
                "new_SGSN_POOL", "new_GGSN", "new_SGW", "new_PGW"};
        String[] selectedColumns = ArrayUtil.add(cols, addedCols);


        ds = ds.withColumn("DeviceTAC" , lpad(ds.col("IMEI"),8,"0"));

        Column cell4gRefCond = ds.col("RAT").equalTo(6).and(ds.col("ECI").substr(0,5).equalTo(location4gRef.col("ECGI3_hex")))
                .and(ds.col("ECI").substr(-2,2).equalTo(location4gRef.col("ECGI4_hex")));

        Column cell3gRefCond = ds.col("RAT").equalTo(1).and(location3gRef.col("LAC3g_hex").equalTo(ds.col("LAC")).and(ds.col("SAC").equalTo(location3gRef.col("SAC3g_hex"))));

        Column cell2gRefCond = ds.col("RAT").equalTo(2).and(location2gRef.col("LAC2g_hex").equalTo(ds.col("LAC")).and(location2gRef.col("CID_hex").equalTo(ds.col("CI"))));

        Column deviceCond = ds.col("DeviceTAC").equalTo(deviceRef.col("TAC_padded"));

        Column protocolRefCond = ds.col("PROT_TYPE").equalTo(protocolRef.col("Protocol_ID"));

        Column sgsn_cond_rat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("SGSN_User_IP").equalTo(sgsnRef.col("User_Plane_IP_Address"))));
        Column ggsn_cond_rat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("GGSN_User_IP").equalTo(col("ggsnRef2.User_Plane_IP_Address"))));
        Column ggsn_cond_rat1 = ((ds.col("RAT").equalTo(lit(1))).and(ds.col("GGSN_User_IP").equalTo(col("ggsnRef3.User_Plane_IP_Address"))));
        Column sgw_cond_rat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGSN_User_IP").equalTo(col("sgwRef.ip"))));
        Column pgw_cond_rat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("GGSN_User_IP").equalTo(col("pgwRef.ip"))));
//        Column failureRefCond = (ds.col("FST_ERROR_CODE").equalTo(failureRef.col("Failure_Cause_Code")));

        ds = ds.join(broadcast(location4gRef.alias("loc4gRef")),cell4gRefCond,"leftouter")
                .join(broadcast(location3gRef.alias("loc3gRef")),cell3gRefCond,"leftouter")
                .join(broadcast(location2gRef.alias("loc2gRef")),cell2gRefCond,"leftouter")
                .join(broadcast(deviceRef.alias("deviceRef")),deviceCond,"leftouter")
                .join(broadcast(protocolRef.alias("protocolRef")),protocolRefCond,"leftouter")
                .join(broadcast(sgsnRef), sgsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsn_cond_rat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgw_cond_rat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgw_cond_rat6, "leftouter")
//                .join(broadcast(failureRef.as("failureRef")), failureRefCond, "leftouter")

                .withColumn("new_City",coalesce(location2gRef.col("City"),location3gRef.col("City"),location4gRef.col("City")))
                .withColumn("new_Province", coalesce(location2gRef.col("Province"),location3gRef.col("Province"),location4gRef.col("Province")))
                .withColumn("new_Region",coalesce(location2gRef.col("region"),location3gRef.col("region"),location4gRef.col("region")))
                .withColumn("new_Country",coalesce(location2gRef.col("country"),location3gRef.col("country"),location4gRef.col("country")))
                .withColumn("new_Cell_Name",coalesce(location2gRef.col("Cell_Name"),location3gRef.col("Cell_Name"),location4gRef.col("Cell_Name")))
                .withColumn("BSC_Name",location2gRef.col("BSC"))
                .withColumn("SITE_Name",location2gRef.col("SITE_Name"))
                .withColumn("Latitude",coalesce(location2gRef.col("LAT"),location3gRef.col("LAT"),location4gRef.col("LAT")))
                .withColumn("Longitude",coalesce(location2gRef.col("LNG"),location3gRef.col("LNG"),location4gRef.col("LNG")))
                .withColumn("RNC_Name",location3gRef.col("RNC"))
                .withColumn("NodeB_Name",location3gRef.col("NODEB_LABEL"))
                .withColumn("ENodeB_Name",location4gRef.col("ENODEB_Name"))
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
//                .withColumn("Failure_Cause_Category" ,coalesce(failureRef.col("Failure_Class"),lit("Unknown")))
//                .withColumn("Failure_Scenario" ,coalesce(failureRef.col("Failure_Scene"),lit("Unknown")))
//                .withColumn("Failure_Cause_Description" ,coalesce(failureRef.col("Failure_Cause_Desc"),lit("Unknown")))
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
                .withColumnRenamed("new_PGW","PGW");
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }
}
