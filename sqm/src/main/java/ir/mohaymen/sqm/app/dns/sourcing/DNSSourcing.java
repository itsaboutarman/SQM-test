package ir.mohaymen.sqm.app.dns.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class DNSSourcing extends Sourcing {

    public DNSSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }


    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> deviceRef = getReference("deviceRef");
        Dataset<Row> location4gRef = getReference("location4gRef");
        Dataset<Row> location3gRef = getReference("location3gRef");
        Dataset<Row> location2gRef = getReference("location2gRef");
        Dataset<Row> protocolRef = getReference("protocolRef");
        Dataset<Row> ggsnRef = getReference("ggsnRef");
        Dataset<Row> sgsnRef = getReference("sgsnRef");
        Dataset<Row> sgw_pgw_ref = getReference("sgwPgwRef");
        Dataset<Row> dnsServerRef = getReference("dnsServerRef");

        // NE Information
        sgsnRef = sgsnRef.withColumnRenamed("NE_Name", "SGSN").withColumnRenamed("Home_SGSN_Pool_Name", "SGSN_POOL");
        ggsnRef = ggsnRef.withColumnRenamed("NE_Name", "GGSN");
        Dataset<Row> sgwRef = sgw_pgw_ref.withColumnRenamed("NE_Name", "SGW");
        Dataset<Row> pgwRef = sgw_pgw_ref.withColumnRenamed("NE_Name", "PGW");
        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();

        // Device Information
        deviceRef = deviceRef.select("TAC", "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME",
                        "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME")
                .withColumn("TAC_padded", lpad(deviceRef.col("TAC"), 8, "0"))
                .drop("TAC");
        deviceRef.persist();

        // Location Information
        location4gRef = location4gRef.withColumn("ECGI_3",
                        hex(split(col("ECGI"), "-").getItem(2).cast(DataTypes.IntegerType)))
                .withColumn("ECGI_4",
                        hex(split(col("ECGI"), "-").getItem(3).cast(DataTypes.IntegerType)));
        location3gRef = location3gRef.withColumn("LAC_3g", hex(col("LAC").cast(DataTypes.IntegerType)))
                .withColumn("SAC_3g", hex(col("SAC").cast(DataTypes.IntegerType)))
                .drop("LAC").drop("SAC");
        location2gRef = location2gRef.withColumn("LAC_2g", hex(col("LAC").cast(DataTypes.IntegerType)))
                .withColumn("CID_2g", hex(col("CID").cast(DataTypes.IntegerType)))
                .drop("LAC").drop("CID");
        location2gRef.persist();
        location3gRef.persist();
        location4gRef.persist();

        // Protocol Information
        protocolRef = protocolRef.select("Protocol", "Protocol_ID", "Protocol_Category");
        protocolRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("BEGIN_TIME")))
             .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);

        // DNS Server Information
        dnsServerRef = dnsServerRef.withColumnRenamed("DNS_Server_Name", "Ref_DNS_Server_Name");
        dnsServerRef.persist();

        String[] cols = ds.columns();
        String[] addedCols = {"new_City", "new_Province", "new_Region", "new_Country", "Protocol", "Protocol_Category", "Brand", "Model"
                , "Technology", "Type", "OS", "SGSN", "SGSN_POOL", "GGSN", "SGW", "PGW", "eNodeB_Name", "new_Cell_Name",
                "SITE_Name", "RNC_Name",
                "NodeB_Name", "BSC_Name", "Longitude", "Latitude", "DNS_Server_Name"};
        String[] selectedColumns = ArrayUtil.add(cols, addedCols);

        ds = ds.withColumn("ECI_1", substring(ds.col("ECI"), 1, 5))
                .withColumn("ECI_2", substring(ds.col("ECI"), 6, 2));
        //Get DeviceTAC
        ds = ds.withColumn("DeviceTAC",
                when(length(col("IMEI")).lt(8),
                        lpad(col("IMEI"), 8, "0")) //Add 0 to start of IMEI
                        .otherwise(substring(col("IMEI"), 1, 8))
        );


        Column protocol_cond = (ds.col("PROT_TYPE").equalTo(protocolRef.col("Protocol_ID")));

        ds = ds.withColumn("main_NE_ID",
                when(ds.col("LAC").isNotNull()
                                .and(ds.col("CI").isNotNull().or(ds.col("SAC").isNotNull()))
                        , concat(ds.col("LAC"), lit("/"),
                                when(ds.col("SAC").isNotNull(), ds.col("SAC"))
                                        .otherwise(ds.col("CI"))))
                        .otherwise(null));

        Column locationCond4g = ds.col("ECI").isNotNull().and(ds.col("RAT").equalTo(6))
                .and(ds.col("ECI_1").equalTo(col("loc4gRef.ECGI_3"))).and(
                        ds.col("ECI_2").equalTo(col("loc4gRef.ECGI_4"))
                )
        ;

        Column locationCond3g = ds.col("LAC").isNotNull().and(ds.col("SAC").isNotNull())
                .and(ds.col("RAT").equalTo(1)).and(
                        ds.col("LAC").equalTo(col("loc3gRef.LAC_3g")))
                                .and(ds.col("SAC").equalTo(col("loc3gRef.SAC_3g")))
                ;

        Column locationCond2g = ds.col("LAC").isNotNull().and(ds.col("CI").isNotNull())
                .and(ds.col("RAT").equalTo(2)).and(
                        ds.col("LAC").equalTo(col("loc2gRef.LAC_2g")))
                                .and(ds.col("CI").equalTo(col("loc2gRef.CID_2g")))
                ;

        Column deviceCond = ds.col("DeviceTAC").equalTo(col("dev_ref.TAC_padded"));

        Column sgsn_cond_rat2 = ((ds.col("RAT").equalTo(lit('2'))).and(ds.col("SGSN_USER_IP").equalTo(sgsnRef.col("User_Plane_IP_Address"))));
        Column ggsn_cond_rat2 = ((ds.col("RAT").equalTo(lit('2'))).and(ds.col("GGSN_USER_IP").equalTo(col("ggsnRef.User_Plane_IP_Address"))));
        Column ggsn_cond_rat1 = ((ds.col("RAT").equalTo(lit('1'))).and(ds.col("GGSN_USER_IP").equalTo(col("ggsnRef.User_Plane_IP_Address"))));
        Column ggsn_cond = ggsn_cond_rat2.or(ggsn_cond_rat1);
        Column sgw_cond_rat6 = ((ds.col("RAT").equalTo(lit('6'))).and(ds.col("SGSN_USER_IP").equalTo(col("sgwRef.ip"))));
        Column pgw_cond_rat6 = ((ds.col("RAT").equalTo(lit('6'))).and(ds.col("GGSN_USER_IP").equalTo(col("pgwRef.ip"))));

        Column dnsServerCond = ds.col("SERVER_IP").equalTo(col("dnsServerRef.Host_IP"));

        ds = ds.join(broadcast(deviceRef.as("dev_ref")), deviceCond, "leftouter")
                .join(broadcast(location4gRef.as("loc4gRef")), locationCond4g, "leftouter")
                .join(broadcast(location3gRef.as("loc3gRef")), locationCond3g, "leftouter")
                .join(broadcast(location2gRef.as("loc2gRef")), locationCond2g, "leftouter")
                .join(broadcast(protocolRef.as("protocol_ref")), protocol_cond, "leftouter")
                .join(broadcast(sgsnRef.as("sgsnRef")), sgsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef")), ggsn_cond, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgw_cond_rat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgw_cond_rat6, "leftouter")
                .join(broadcast(dnsServerRef.as("dnsServerRef")), dnsServerCond, "leftouter")
                .withColumn("new_Cell_Name",coalesce(location4gRef.col("Cell Name"),location3gRef.col("Cell Name"),location2gRef.col("Cell Name")))
                .withColumn("BSC_Name",location2gRef.col("BSC"))
                .withColumn("SITE_Name",location2gRef.col("SITE Name"))
                .withColumn("Latitude",coalesce(location2gRef.col("LAT"),location3gRef.col("LAT"),location4gRef.col("LAT")).cast(DataTypes.StringType))
                .withColumn("Longitude",coalesce(location2gRef.col("LNG"),location3gRef.col("LNG"),location4gRef.col("LNG")).cast(DataTypes.StringType))
                .withColumn("RNC_Name",location3gRef.col("RNC"))
                .withColumn("NodeB_Name",location3gRef.col("NODEB_LABEL"))
                .withColumn("eNodeB_Name",location4gRef.col("ENODEB Name"))
                .withColumn("Brand", coalesce(col("dev_ref.TER_BRAND_NAME"), lit("Unknown")))
                .withColumn("Model", coalesce(col("dev_ref.TER_MODEL_NAME"), lit("Unknown")))
                .withColumn("Technology", coalesce(col("dev_ref.TER_MODENAME"), lit("Unknown")))
                .withColumn("Type", coalesce(col("dev_ref.TER_TYPE_NAME_EN"), lit("Unknown")))
                .withColumn("OS", coalesce(col("dev_ref.TER_OS_TYPE_NAME"), lit("Unknown")))
                .withColumn("DNS_Server_Name", coalesce(col("dnsServerRef.Ref_DNS_Server_Name"), lit("Unknown")))
                .withColumn("new_City", coalesce(col("loc4gRef.City"), col("loc3gRef.City"), col("loc2gRef.City")))
                .withColumn("new_Province", coalesce(col("loc4gRef.Province"), col("loc3gRef.Province"), col("loc2gRef.Province")))
                .withColumn("new_Region", coalesce(col("loc4gRef.Region"), col("loc3gRef.Region"), col("loc2gRef.Region")))
                .withColumn("new_Country", coalesce(col("loc4gRef.Country"), col("loc3gRef.Country"), col("loc2gRef.Country")))
                .selectExpr(selectedColumns)
                .withColumnRenamed("new_City", "City")
                .withColumnRenamed("new_Province", "Province")
                .withColumnRenamed("new_Region", "Region")
                .withColumnRenamed("new_Country", "Country")
                .withColumnRenamed("new_Cell_Name","Cell_Name");
        System.out.println("output schema inside sourcing:");
        ds.printSchema();
        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }


}