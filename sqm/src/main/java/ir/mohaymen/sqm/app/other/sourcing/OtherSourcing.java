package ir.mohaymen.sqm.app.other.sourcing;

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

import javax.xml.crypto.Data;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class OtherSourcing extends Sourcing{
    public OtherSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds.printSchema();
        Dataset<Row> deviceRef = references.get("deviceRef");
        Dataset<Row> location2gRef = references.get("location2gRef");
        Dataset<Row> location3gRef = references.get("location3gRef");
        Dataset<Row> location4gRef = references.get("location4gRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");
        Dataset<Row> protocolRef = references.get("protocolRef");
        Dataset<Row> appRef = references.get("appRef");
        Dataset<Row> apnDashboardRef = references.get("apnDashboardRef");


        appRef = appRef.select("Application_ID" , "Application_Name");
        appRef.persist();
        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // Location Information
        location4gRef = location4gRef.select("City","Province","region","country","ENODEB_ID",
                "Cell_Name","AZIMUTH","Cell_ID","LAT","LNG");
        location3gRef = location3gRef.select("LAC","SAC","City","Province","region","country","RNC",
                        "NODEB_LABEL","LAT","LNG" , "Service_Area_Name" ,"AZIMUTH")
                .withColumnRenamed("LAC" ,"LAC3g");
        location3gRef = location3gRef.withColumn("LAC3g_hex" ,
                        hex(location3gRef.col("LAC3g").cast(DataTypes.IntegerType)))
                .withColumn("SAC3g_hex" , hex(location3gRef.col("SAC").cast(DataTypes.IntegerType)))
                .drop("SAC");

        location2gRef =  location2gRef.select("LAC","CID", "City", "Province", "region", "country"
                , "Cell_Name", "BSC", "SITE_Name", "LAT", "LNG" , "AZIMUTH" ,"VENDOR");
        location2gRef = location2gRef
                .withColumnRenamed("LAC","LAC2g")
                .withColumnRenamed("CID","CID2g")
                .withColumn("LAC2g_hex" , hex(col("LAC2g").cast(DataTypes.IntegerType)))
                .withColumn("CID_hex" , hex(col("CID2g").cast(DataTypes.IntegerType)));

        location4gRef.persist();
        location3gRef.persist();
        location2gRef.persist();

        apnDashboardRef.persist();

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

        // Protocol Information
        protocolRef =  protocolRef.select("Protocol_ID", "Protocol_Name");
        protocolRef.persist();

        // Partition Column
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("BEGIN_TIME")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // Selected columns
        String[] cols = ds.columns();
        String[] addedCols = {"Brand","Model","Technology", "Type", "OS",
                "new_City", "new_Province", "new_Region", "new_Country", "new_Cell_Name", "BSC_Name",
                "SITE_Name", "Latitude", "Longitude", "RNC_Name","NodeB_Name", "new_SGSN", "new_SGSN_POOL", "new_GGSN", "new_SGW", "new_PGW",
                "Protocol_Name","Protocol_Category_Name", "Application_Name" ,"VENDOR" , "new_AZIMUTH","ENODEB_ID","APN_Edit"};
        String[] selectedColumns = ArrayUtil.add(cols, addedCols);

        // Device conditions
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).lt(8),
                        lpad(col("IMEI"), 8, "0"))
                        .otherwise(substring(col("IMEI"), 1, 8))
        );

        ds = ds.withColumn("realCID",
                when(col("CI").isNotNull().and(
                        length(col("CI")).gt(6)
                ), col("CI").cast("long").mod(256)));

        // Location conditions
        ds = ds.withColumn(
                        "realCID",
                        when(
                                col("CI").isNotNull().and(length(col("CI")).gt(6)),
                                col("CI").cast("long").bitwiseAND(255)
                        ).otherwise(lit(null))
                )
                .withColumn(
                        "eNodeB",
                        when(
                                col("CI").isNotNull().and(length(col("CI")).gt(6)),
                                col("CI").cast("long").divide(256)
                        ).otherwise(lit(null))
                );

        Column cell4gRefCond = ds.col("realCID").equalTo(location4gRef.col("ENODEB_ID").cast(DataTypes.IntegerType))
                .and(ds.col("eNodeB").equalTo(location4gRef.col("Cell_ID").cast(DataTypes.IntegerType)));

//        Column cell4gRefCond = ds.col("RAT").equalTo(6)
//                .and(ds.col("ECI").substr(0,5).equalTo(hex(location4gRef.col("ENODEB_ID").cast(DataTypes.IntegerType))))
//                .and(ds.col("ECI").substr(-2,2).equalTo(hex(location4gRef.col("Cell_ID").cast(DataTypes.IntegerType))));
        ds = ds.withColumn(
                        "LAC_hex",
                        when(
                                ds.col("CI").isNotNull(),
                                hex(ds.col("LAC").cast(DataTypes.IntegerType))
                        )
                )
                .withColumn(
                        "SAC_hex",
                        when(
                                ds.col("CI").isNotNull(),
                                hex(ds.col("SAC").cast(DataTypes.IntegerType))
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

        Column deviceCond = ds.col("DeviceTAC").equalTo(deviceRef.col("TAC_padded"));

        // NE conditions
        Column sgsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("SGSN_USER_IP").equalTo(col("sgsnRef.User_Plane_IP_Address"))));
        Column ggsnCondRat2 = ((ds.col("RAT").equalTo(lit(2))).and(ds.col("GGSN_USER_IP").equalTo(col("ggsnRef2.User_Plane_IP_Address"))));
        Column ggsnCondRat1 = ((ds.col("RAT").equalTo(lit(1))).and(ds.col("GGSN_USER_IP").equalTo(col("ggsnRef3.User_Plane_IP_Address"))));
        Column sgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("SGSN_USER_IP").equalTo(col("sgwRef.ip"))));
        Column pgwCondRat6 = ((ds.col("RAT").equalTo(lit(6))).and(ds.col("GGSN_USER_IP").equalTo(col("pgwRef.ip"))));

        Column protocolCond =  ds.col("PROT_TYPE").equalTo(col("protocolRef.Protocol_ID"));

        Column appCond = ds.col("APP_ID").equalTo(appRef.col("Application_ID"));

        ds = applyAPNStandardization(ds);
        Column apnDashboardRefCond = ds.col("APN").equalTo(apnDashboardRef.col("value"));

        ds = ds.join(broadcast(location4gRef.as("loc4gRef")),cell4gRefCond,"leftouter")
                .join(broadcast(location3gRef.as("loc3gRef1")), cell3gRefCond1 ,"leftouter")
                .join(broadcast(location3gRef.as("loc3gRef2")), cell3gRefCond2 ,"leftouter")
                .join(broadcast(location2gRef.as("loc2gRef")),cell2gRefCond,"leftouter")
                .join(broadcast(deviceRef.as("deviceRef")),deviceCond,"leftouter")
                .join(broadcast(protocolRef.as("protocolRef")), protocolCond,"leftouter")
                .join(broadcast(sgsnRef.as("sgsnRef")), sgsnCondRat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsnCondRat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsnCondRat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgwCondRat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgwCondRat6, "leftouter")
                .join(broadcast(appRef.as("appRef")), appCond, "leftouter")
                .join(broadcast(apnDashboardRef.as("apnDashboardRef")), apnDashboardRefCond, "leftouter")

                //App
                .withColumn("Application_Name", coalesce(appRef.col("Application_Name") ,lit("Unknown")))
                // Device
                .withColumn("Brand" , coalesce(deviceRef.col("TER_BRAND_NAME") , lit("Unknown")))
                .withColumn("Model",coalesce(deviceRef.col("TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology",coalesce(deviceRef.col("TER_MODENAME"), lit("Unknown")))
                .withColumn("Type",coalesce(deviceRef.col("TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS",coalesce(deviceRef.col("TER_OS_TYPE_NAME"), lit("Unknown")))

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
                // .withColumn("ENodeB_Name",location4gRef.col("ENODEB_Name"))

                // APN
                .withColumn("APN_Edit",
                        coalesce(when(apnDashboardRef.col("value").isNotNull(), ds.col("APN"))
                                .otherwise(lit("Unknown")), lit("Unknown"))
                )

                // NE
                .withColumn("new_SGSN" ,sgsnRef.col("SGSN"))
                .withColumn("new_SGSN_POOL" ,sgsnRef.col("SGSN_POOL"))
                .withColumn("new_GGSN" , coalesce(col("ggsnRef2.GGSN"),col("ggsnRef3.GGSN")))
                .withColumn("new_SGW" ,col("sgwRef.SGW"))
                .withColumn("new_PGW" ,col("pgwRef.PGW"))

                // Protocol
                .withColumn("Protocol_Name" , coalesce(col("protocolRef.Protocol_Name") , lit("Unknown")))
                .withColumn("Protocol_Category_Name" , lit("Other"))

                .selectExpr(selectedColumns)
                .withColumnRenamed("new_City","City")
                .withColumnRenamed("new_Province","Province")
                .withColumnRenamed("new_Region","Region")
                .withColumnRenamed("new_Country","Country")
                .withColumnRenamed("new_Cell_Name","CELL_Name")
                .withColumnRenamed("new_AZIMUTH" ,"AZIMUTH")
                .withColumnRenamed("new_SGSN","SGSN")
                .withColumnRenamed("new_SGSN_POOL","SGSN_POOL")
                .withColumnRenamed("new_GGSN","GGSN")
                .withColumnRenamed("new_SGW","SGW")
                .withColumnRenamed("new_PGW","PGW");


        // Unknown conditions (Geograpgic columns & APN)

        ds = ds.withColumn("APN", coalesce(col("APN") ,lit("Unknown")))
                .withColumn("City", coalesce(col("City") ,lit("Unknown")))
                .withColumn("Province", coalesce(col("Province") ,lit("Unknown")))
                .withColumn("Region", coalesce(col("Region") ,lit("Unknown")))
                .withColumn("Country", coalesce(col("Country") ,lit("Unknown")));

        // RAT Unknown condition
        ds = ds.withColumn("ENODEB_ID", when(col("RAT").equalTo(6), coalesce(col("ENODEB_ID"), lit("Unknown"))).when(col("RAT").isNull(), lit("Unknown")))
                .withColumn("RNC_Name", when(col("RAT").equalTo(1), coalesce(col("RNC_Name"), lit("Unknown"))))
                .withColumn("BSC_Name", when(col("RAT").equalTo(2), coalesce(col("BSC_Name"), lit("Unknown"))));

        ds.printSchema();
        return ds;
    }

    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> ds) {
        return ds.partitionBy("day");
    }


    private Dataset<Row> applyAPNStandardization(Dataset<Row> ds){
        Column cleanAPN = col("APN");
        // remove control chars
        cleanAPN = regexp_replace(cleanAPN,
                "[\\x00-\\x1F\\x7F-\\x9F]", "");
        // remove unwanted patterns
        String[] patterns = {
                "MNC\\d{2,3}",
                "MCC\\d{2,3}",
                "GPRS[\\?O]?",
                "\\d{2,3}",
                "IPV[46]",
                "INTERNET",
                "V\\d+(\\.\\d+)?"
        };

        for (String p : patterns) {
            cleanAPN = regexp_replace(cleanAPN, "(?i)" + p, " ");
        }
        // replace special chars with dot
        cleanAPN = regexp_replace(cleanAPN,
                "[@،;:|/\\\\$#&*()\\[\\]{}~`\\-_+=><]", ".");

        // replace non-alphanumeric except dot
        cleanAPN = regexp_replace(cleanAPN,
                "[^a-zA-Z0-9\\.]", ".");

        // lowercase
        cleanAPN = lower(cleanAPN);
// Normalize whitespace first (important!)
        cleanAPN = regexp_replace(cleanAPN, "\\s+", " ");
        cleanAPN = trim(cleanAPN);

// Convert spaces to dots
        cleanAPN = regexp_replace(cleanAPN, " ", ".");
        // collapse multiple dots
        cleanAPN = regexp_replace(cleanAPN, "\\.+", ".");

        // trim dots
        cleanAPN = regexp_replace(cleanAPN, "^\\.|\\.$", "");
        Column parts = split(cleanAPN, "\\.");


        // remove single-character segments
        Column filteredParts = expr(
                "filter(" +
                        "split(" + cleanAPN.expr().sql() + ", '\\\\.'), " +
                        "x -> length(x) > 1" +
                        ")"
        );

        // 1️⃣0️⃣ Rebuild APN
        Column rebuilt = concat_ws(".", filteredParts);

        // 1️⃣1️⃣ Remove duplicate suffixes (.mci.mci → .mci)
        rebuilt = regexp_replace(rebuilt, "(\\.mci)+", ".mci");
        rebuilt = regexp_replace(rebuilt, "(\\.irancell)+", ".irancell");

        ds = ds.withColumn("APN", rebuilt);

        // =========================
        // Extract APN Group
        // =========================
        //   ds.show(100);
        //   String domainsRegex = "^(mci|irancell|mtn|rightel|tci|tat|ir|com|net|org)$";

//        Column apnGroup =
//                element_at(
//                        expr("filter(split(clean_APN,'\\\\.'), x -> NOT x RLIKE '" + domainsRegex + "')"),
//                        1
//                );
//
//        ds = ds.withColumn("apn_group",
//                when(apnGroup.isNotNull(), apnGroup)
//                        .otherwise(element_at(split(col("clean_APN"), "\\."), 1))
        // );

        return ds;
    }
}