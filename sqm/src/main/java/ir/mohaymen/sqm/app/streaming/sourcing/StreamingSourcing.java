package ir.mohaymen.sqm.app.streaming.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Map;
import static org.apache.spark.sql.functions.*;


public class StreamingSourcing extends Sourcing {

    public StreamingSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }


    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {

        Dataset<Row> sgsnRef = references.get("sgsnRef");
        Dataset<Row> ggsnRef = references.get("ggsnRef");
        Dataset<Row> tacRef = references.get("tacRef");
        Dataset<Row> locationRef = references.get("locationRef");
        Dataset<Row> protocolRef = references.get("protocolRef");
        Dataset<Row> protocolCategoryRef = references.get("protocolCategoryRef");
        Dataset<Row> sgwPgwRef = references.get("sgwPgwRef");
        Dataset<Row> cell2gRef = references.get("cell2gRef");
        Dataset<Row> cell3gRef = references.get("cell3gRef");
        Dataset<Row> cell4gRef = references.get("cell4gRef");

        sgsnRef = sgsnRef.withColumnRenamed("NE_Name","SGSN")
                .withColumnRenamed("Home_SGSN_Pool_Name","SGSN_POOL");
        ggsnRef = ggsnRef.withColumnRenamed("NE_Name","GGSN");
        Dataset<Row> sgwRef = sgwPgwRef.withColumnRenamed("NE_Name","SGW");
        Dataset<Row> pgwRef = sgwPgwRef.withColumnRenamed("NE_Name","PGW");

        Dataset<Row> refEcgi = locationRef.where(col("NE_Type").equalTo("ECGI"));
        locationRef = locationRef.where(col("NE_Type").notEqual("ECGI"));


        cell2gRef = cell2gRef.withColumnRenamed("LAC","LAC2g")
                .withColumnRenamed("Cell_Name","Cell_Name2g");
        cell3gRef = cell3gRef.withColumnRenamed("LAC","LAC3g");
        cell4gRef = cell4gRef.withColumnRenamed("Cell_Name","Cell_Name4g");

        locationRef.persist();
        refEcgi.persist();
        protocolRef = protocolRef.select("Protocol", "Protocol_ID")
                .withColumnRenamed("Protocol","Protocol_name");
        protocolRef.persist();
        protocolCategoryRef = protocolCategoryRef.select("Protocol", "Protocol_Category_ID")
                .withColumnRenamed("Protocol","Protocol_Category_name");
        protocolCategoryRef.persist();
        tacRef = tacRef.withColumn("TAC",lpad(col("TAC"),8,"0")).withColumnRenamed("TAC","TAC_ID");
        tacRef.persist();

        ggsnRef.persist();
        sgsnRef.persist();
        sgwRef.persist();
        pgwRef.persist();

        cell2gRef.persist();
        cell3gRef.persist();
        cell4gRef.persist();

        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("Start_Time_s")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);


        Column location_cond1 = ((ds.col("ECI").isNotNull()).and(ds.col("ECI").equalTo(col("ref_ecgi.NE_ID_3"))));
        Column location_cond2 = ((ds.col("ECI").isNull()).and(ds.col("LAC").isNotNull()).and(ds.col("SAC").isNotNull()).and(ds.col("LAC").equalTo(col("new_ref2.NE_ID_3"))).and(ds.col("SAC").equalTo(col("new_ref2.NE_ID_4"))));
        Column location_cond3 = ((ds.col("ECI").isNull()).and(ds.col("LAC").isNotNull()).and(ds.col("CI").isNotNull()).and(ds.col("LAC").equalTo(col("new_ref3.NE_ID_3"))).and(ds.col("CI").equalTo(col("new_ref3.NE_ID_4"))));

        Column cell4g_cond = ((ds.col("ECI").isNotNull()).and(substring(ds.col("ECI"), 1, 5).equalTo(cell4gRef.col("eNodeB_ID_Hex"))).and(substring(ds.col("ECI"), -2, 2).equalTo(cell4gRef.col("Cell_ID_Hex"))));
        Column cell3g_cond = ((ds.col("ECI").isNull()).and(ds.col("LAC").isNotNull()).and(ds.col("SAC").isNotNull()).and(ds.col("LAC").equalTo(cell3gRef.col("LAC3g"))).and(ds.col("SAC").equalTo(cell3gRef.col("Service_Area_Code"))));
        Column cell2g_cond = ((ds.col("ECI").isNull()).and(ds.col("LAC").isNotNull()).and(ds.col("CI").isNotNull()).and(ds.col("LAC").equalTo(cell2gRef.col("LAC2g"))).and(ds.col("CI").equalTo(cell2gRef.col("Cell_Code"))));

        Column protocol_cond = (ds.col("Protocol").equalTo(protocolRef.col("Protocol_ID")));
        Column protocol_category_cond = (ds.col("Protocol_Category").equalTo(protocolCategoryRef.col("Protocol_Category_ID")));
        Column tac_cond = (substring(ds.col("IMEI"), 1, 8).equalTo(tacRef.col("TAC_ID")));

        Column sgsn_cond_rat2 = ((ds.col("RAT").equalTo(lit('2'))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(sgsnRef.col("User_Plane_IP_Address"))));
        Column ggsn_cond_rat2 = ((ds.col("RAT").equalTo(lit('2'))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef2.User_Plane_IP_Address"))));
        Column ggsn_cond_rat1 = ((ds.col("RAT").equalTo(lit('1'))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("ggsnRef3.User_Plane_IP_Address"))));

        Column sgw_cond_rat6 = ((ds.col("RAT").equalTo(lit('6'))).and(ds.col("SGSN_User_Plane_IP_Address").equalTo(col("sgwRef.User_Plane_IP_Address"))));
        Column pgw_cond_rat6 = ((ds.col("RAT").equalTo(lit('6'))).and(ds.col("GGSN_User_Plane_IP_Address").equalTo(col("pgwRef.User_Plane_IP_Address"))));


        String[] cols = ds.columns();

        for (String col : new String[]{"new_city", "new_province", "new_region", "new_country", "Protocol_name", "Protocol_Category_name",
                "TER_BRAND_NAME", "TER_MODEL_NAME", "TER_MODENAME", "TER_TYPE_NAME_EN", "TER_OS_TYPE_NAME", "SGSN", "SGSN_POOL", "full_ggsn", "SGW", "PGW",
                "eNodeB_Name", "Cell_Name", "RNC_Name", "Service_Area_Name", "NodeB_Name", "BSC_Name", "BTS_Name", "new_Longitude", "new_Latitude"}) {
            cols = Arrays.copyOf(cols, cols.length + 1);
            cols[cols.length - 1] = col;
        }

        ds = ds
                .join(broadcast(locationRef.as("new_ref3")), location_cond3, "leftouter")
                .join(broadcast(locationRef.as("new_ref2")), location_cond2, "leftouter")
                .join(broadcast(refEcgi.as("ref_ecgi")), location_cond1, "leftouter")
                .join(broadcast(protocolRef), protocol_cond, "leftouter")
                .join(broadcast(protocolCategoryRef), protocol_category_cond, "leftouter")
                .join(broadcast(tacRef), tac_cond, "leftouter")
                .join(broadcast(sgsnRef), sgsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef2")), ggsn_cond_rat2, "leftouter")
                .join(broadcast(ggsnRef.as("ggsnRef3")), ggsn_cond_rat1, "leftouter")
                .join(broadcast(sgwRef.as("sgwRef")), sgw_cond_rat6, "leftouter")
                .join(broadcast(pgwRef.as("pgwRef")), pgw_cond_rat6, "leftouter")
                .join(broadcast(cell4gRef), cell4g_cond, "leftouter")
                .join(broadcast(cell3gRef), cell3g_cond, "leftouter")
                .join(broadcast(cell2gRef), cell2g_cond, "leftouter")
                .withColumn("new_city", coalesce(col("ref_ecgi.City"), col("new_ref2.City"),col("new_ref3.City")))
                .withColumn("new_province", coalesce(col("ref_ecgi.Province"), col("new_ref2.Province"), col("new_ref3.Province")))
                .withColumn("new_region", coalesce(col("ref_ecgi.Region"), col("new_ref2.Region"), col("new_ref3.Region")))
                .withColumn("new_country", coalesce(col("ref_ecgi.Country"), col("new_ref2.Country"), col("new_ref3.Country")))
                .withColumn("full_ggsn", coalesce(col("ggsnRef2.GGSN"), col("ggsnRef3.GGSN")))
                .withColumn("Cell_Name", coalesce(cell4gRef.col("Cell_Name4g"), cell2gRef.col("Cell_Name2g")))
                .withColumn("new_Longitude", coalesce(cell4gRef.col("Longitude"), cell3gRef.col("Longitude"), cell2gRef.col("Longitude")))
                .withColumn("new_Latitude", coalesce(cell4gRef.col("Latitude"), cell3gRef.col("Latitude"), cell2gRef.col("Latitude")))
                .selectExpr(cols)
                .withColumnRenamed("new_city", "City")
                .withColumnRenamed("new_province", "Province")
                .withColumnRenamed("new_region", "Region")
                .withColumnRenamed("new_country", "Country")
                .withColumnRenamed("full_ggsn", "GGSN")
                .withColumnRenamed("new_Longitude", "Longitude")
                .withColumnRenamed("new_Latitude", "Latitude");


        return ds;
    }


    @Override
    protected DataStreamWriter<Row> partitionData(DataStreamWriter<Row> dw) {
        dw = dw.partitionBy("day");
        return dw;
    }

}
