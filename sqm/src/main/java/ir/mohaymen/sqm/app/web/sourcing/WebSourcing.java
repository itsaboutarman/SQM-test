package ir.mohaymen.sqm.app.web.sourcing;

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
import org.apache.spark.sql.types.DataTypes;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Job
public class WebSourcing extends Sourcing {
    public WebSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
//        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
        return ds;
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> appRef = references.get("appRef");
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> location4gRef = getReference("location4gRef");
        Dataset<Row> location3gRef = getReference("location3gRef");
        Dataset<Row> location2gRef = getReference("location2gRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");
        Dataset<Row> protocolRef = getReference("protocolRef");

        // ------------------------------- Location -------------------------------
        location4gRef = location4gRef.select("City","Province","region","country","ENODEB_ID",
                "Cell_Name","AZIMUTH","Cell_ID","LAT","LNG")
                .withColumn(
                        "ENODEB_ID",
                        expr("try_cast(ENODEB_ID AS INT)")
                )
                .withColumn(
                        "Cell_ID",
                        expr("try_cast(Cell_ID AS INT)")
                )
                .withColumn("AZIMUTH", expr("try_cast(AZIMUTH AS BIGINT)"));
        location3gRef = location3gRef.select("LAC","SAC","City","Province","region","country","RNC",
                        "NODEB_LABEL","LAT","LNG" , "Service_Area_Name" ,"AZIMUTH")
                .withColumnRenamed("LAC" ,"LAC3g");
        location3gRef = location3gRef
                .withColumn(
                        "LAC3g_hex",
                        upper(hex(expr("conv(LAC3g, 16, 10)").cast(DataTypes.IntegerType)))
                )
                .withColumn(
                        "SAC3g_hex",
                        upper(hex(expr("conv(SAC, 16, 10)").cast(DataTypes.IntegerType)))
                )
                .drop("SAC");

        location2gRef =  location2gRef.select("LAC","CID", "City", "Province", "region", "country"
                , "Cell_Name", "BSC", "SITE_Name", "LAT", "LNG" , "AZIMUTH" ,"VENDOR");
        location2gRef = location2gRef
                .withColumnRenamed("LAC", "LAC2g")
                .withColumnRenamed("CID", "CID2g")
                .withColumn(
                        "LAC2g_hex",
                        upper(hex(expr("conv(LAC2g, 16, 10)").cast(DataTypes.IntegerType)))
                )
                .withColumn(
                        "CID_hex",
                        upper(hex(expr("conv(CID2g, 16, 10)").cast(DataTypes.IntegerType)))
                );

        location4gRef.persist();
        location3gRef.persist();
        location2gRef.persist();


        // ------------------------------- Application -------------------------------
        appRef = appRef.select("Application_ID" , "Application_Name");
        appRef.persist();


        // ------------------------------- Device Reference -------------------------------
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // ------------------------------- Protocol Information -------------------------------
        protocolRef =  protocolRef.select("Protocol_ID", "Protocol_Name");
        protocolRef.persist();

        // ------------------------------- NE Information -------------------------------
        sgsnRef = sgsnRef.select("NE_Name","Home_SGSN_Pool_Name", "User_Plane_IP_Address")
                .withColumnRenamed("NE_Name", "SGSN")
                .withColumnRenamed("Home_SGSN_Pool_Name", "SGSN_POOL");

        ggsnRef = ggsnRef.select("NE_Name", "User_Plane_IP_Address")
                .withColumnRenamed("NE_Name", "GGSN");

        Dataset<Row> sgwRef = sgw_pgw_ref.select("NE_Name", "ip")
                .withColumnRenamed("NE_Name", "SGW");

        Dataset<Row> pgwRef = sgw_pgw_ref.select("NE_Name", "ip")
                .withColumnRenamed("NE_Name", "PGW");

        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();


        // ------------------------------- Partition Column -------------------------------
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s"))).withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // ------------------------------- Selected columns -------------------------------
        String[] cols = ds.columns();
        String[] addedCols = {"Brand", "Model", "Technology", "Type", "OS",
                "new_City", "new_Province", "new_Region", "new_Country", "new_Cell_Name", "BSC_Name",
                "SITE_Name", "Latitude", "Longitude", "RNC_Name","NodeB_Name", "new_SGSN", "new_SGSN_POOL",
                "new_GGSN", "new_SGW", "new_PGW", "Protocol_Name","Protocol_Category_Name", "Application_Name",
                "VENDOR" , "new_AZIMUTH", "ENODEB_ID"};

        String[] selectedColumns = ArrayUtil.add(cols, addedCols);

        // ------------------------------- CONDITION: application -------------------------------
        Column appCond = ds.col("APP_ID").equalTo(appRef.col("Application_ID"));

        // ------------------------------- CONDITION: Device -------------------------------
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).lt(8), lpad(col("IMEI"), 8, "0"))
                        .otherwise(substring(col("IMEI"), 1, 8))
        );

        // ------------------------------- CONDITION: Location -------------------------------
        ds = ds.withColumn("realCID",
                when(col("CI").isNotNull().and(
                        length(col("CI")).gt(6)
                ), col("CI").cast("long").mod(256)));

        Column ciLong = expr("try_cast(conv(CI, 16, 10) AS BIGINT)");
        ds = ds.withColumn(
                        "realCID",
                        when(
                                col("CI").isNotNull().and(length(col("CI")).gt(6)),
                                ciLong.bitwiseAND(255)
                        ).otherwise(lit(null))
                )
                .withColumn(
                        "eNodeB",
                        when(
                                col("CI").isNotNull().and(length(col("CI")).gt(6)),
                                ciLong.divide(256)
                        ).otherwise(lit(null))
                );

        Column cell4gRefCond = ds.col("realCID").equalTo(location4gRef.col("ENODEB_ID").cast(DataTypes.IntegerType))
                .and(ds.col("eNodeB").equalTo(location4gRef.col("Cell_ID").cast(DataTypes.IntegerType)));

        ds = ds.withColumn(
                        "LAC_hex",
                        when(col("CI").isNotNull(),
                                upper(hex(expr("conv(LAC, 16, 10)").cast(DataTypes.LongType)))
                        )
                )
                .withColumn(
                        "SAC_hex",
                        when(col("CI").isNotNull(),
                                upper(hex(expr("conv(SAC, 16, 10)").cast(DataTypes.LongType)))
                        )
                );

        Column cell3gRefCond1 = coalesce(ds.col("CI"), lit(0)).equalTo(0)
                .and(
                        col("loc3gRef1.LAC3g_hex").equalTo(ds.col("LAC_hex"))
                                .and(col("loc3gRef1.SAC3g_hex").equalTo(ds.col("SAC_hex")))
                );

        Column cell3gRefCond2 = ds.col("CI").isNull().and(
                        ds.col("CI").notEqual(0))
                .and(col("loc3gRef2.LAC3g_hex").equalTo(ds.col("LAC_hex"))
                        .and(col("loc3gRef2.SAC3g_hex").equalTo(ds.col("SAC_hex"))));

        Column cell2gRefCond = ds.col("CI").isNull().and(
                        ds.col("CI").notEqual(0))
                .and(location2gRef.col("LAC2g_hex").equalTo(ds.col("LAC_hex"))
                        .and(location2gRef.col("CID_hex").equalTo(ds.col("SAC_hex"))));

        // ------------------------------- CONDITION: Device -------------------------------
        Column deviceCond = ds.col("DeviceTAC").equalTo(deviceRef.col("TAC_padded"));

        // ------------------------------- CONDITION: Protocol -------------------------------
        Column protocolCond =  ds.col("Protocol").equalTo(col("protocolRef.Protocol_ID"));

        // ------------------------------- CONDITION: NE -------------------------------
        Column sgsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(col("sgsnRef.User_Plane_IP_Address"))));
        Column ggsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef2.User_Plane_IP_Address"))));

        Column ggsnCondRat1 = ((ds.col("RAT").equalTo(lit(1))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef3.User_Plane_IP_Address"))));

        Column sgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(col("sgwRef.ip"))));
        Column pgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("pgwRef.ip"))));


        ds = ds.join(broadcast(appRef.as("appRef")), appCond, "leftouter")
                .join(broadcast(deviceRef.as("deviceRef")),deviceCond,"leftouter")
                .join(broadcast(location4gRef.as("loc4gRef")),cell4gRefCond,"leftouter")
                .join(broadcast(location3gRef.as("loc3gRef1")), cell3gRefCond1 ,"leftouter")
                .join(broadcast(location3gRef.as("loc3gRef2")), cell3gRefCond2 ,"leftouter")
                .join(broadcast(location2gRef.as("loc2gRef")),cell2gRefCond,"leftouter")
                .join(broadcast(protocolRef.as("protocolRef")), protocolCond,"leftouter")
                .join(broadcast(sgsnRef.as("sgsnRef")), sgsnCondRat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsnCondRat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsnCondRat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgwCondRat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgwCondRat6, "leftouter")

                //App
                .withColumn("Application_Name", coalesce(appRef.col("Application_Name").cast(DataTypes.StringType), lit("Unknown")))

                // Device
                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME").cast(DataTypes.StringType), lit("Unknown")))

                // Location
                .withColumn("new_City",coalesce(location2gRef.col("City"),
                        col("loc3gRef1.City"), col("loc3gRef2.City") ,location4gRef.col("City")))
                .withColumn("new_Province", coalesce(location2gRef.col("Province"),
                        col("loc3gRef1.Province"), col("loc3gRef2.Province"), location4gRef.col("Province")))
                .withColumn("new_Region",coalesce(location2gRef.col("region"),
                        col("loc3gRef1.region"), col("loc3gRef2.region"), location4gRef.col("region")))
                .withColumn("new_Country",coalesce(location2gRef.col("country"),
                        col("loc3gRef1.country"), col("loc3gRef2.country"), location4gRef.col("country")))
                .withColumn("new_Cell_Name",coalesce(location2gRef.col("Cell_Name"), location4gRef.col("Cell_Name")))
                .withColumn("Latitude",coalesce(location2gRef.col("LAT"),
                        col("loc3gRef1.LAT"), col("loc3gRef2.LAT"), location4gRef.col("LAT")).cast(DataTypes.DoubleType))
                .withColumn("Longitude",coalesce(location2gRef.col("LNG"),
                        col("loc3gRef1.LNG"), col("loc3gRef2.LNG"), location4gRef.col("LNG")).cast(DataTypes.DoubleType))
                .withColumn("new_AZIMUTH", coalesce(location2gRef.col("AZIMUTH"),
                        col("loc3gRef1.AZIMUTH"), col("loc3gRef2.AZIMUTH"), location4gRef.col("AZIMUTH")))
                .withColumn("BSC_Name", location2gRef.col("BSC"))
                .withColumn("SITE_Name", location2gRef.col("SITE_Name"))
                .withColumn("RNC_Name", coalesce(col("loc3gRef1.RNC"), col("loc3gRef2.RNC")))
                .withColumn("NodeB_Name", coalesce(col("loc3gRef1.NODEB_LABEL"), col("loc3gRef2.NODEB_LABEL")))

                // Protocol
                .withColumn("Protocol_Name" , coalesce(col("protocolRef.Protocol_Name").cast(DataTypes.StringType), lit("Unknown")))
                .withColumn("Protocol_Category_Name" , lit("WEB"))

                // NE
                .withColumn("new_SGSN", sgsnRef.col("SGSN"))
                .withColumn("new_SGSN_POOL", sgsnRef.col("SGSN_POOL"))
                .withColumn("new_GGSN", coalesce(col("ggsnRef2.GGSN"),col("ggsnRef3.GGSN")))
                .withColumn("new_SGW", col("sgwRef.SGW"))
                .withColumn("new_PGW", col("pgwRef.PGW"))

                .selectExpr(selectedColumns)
                .withColumnRenamed("new_City","City")
                .withColumnRenamed("new_Province","Province")
                .withColumnRenamed("new_Region","Region")
                .withColumnRenamed("new_Country","Country")
                .withColumnRenamed("new_Cell_Name", "CELL_Name")
                .withColumnRenamed("new_AZIMUTH", "AZIMUTH")
                .withColumnRenamed("new_SGSN","SGSN")
                .withColumnRenamed("new_SGSN_POOL", "SGSN_POOL")
                .withColumnRenamed("new_GGSN","GGSN")
                .withColumnRenamed("new_SGW","SGW")
                .withColumnRenamed("new_PGW","PGW");

        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }
}
