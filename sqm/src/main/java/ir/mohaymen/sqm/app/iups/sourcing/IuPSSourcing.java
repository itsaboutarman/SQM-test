package ir.mohaymen.sqm.app.iups.sourcing;

import ir.mohaymen.sqm.app.Sourcing;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Column;

import java.util.HashMap;
import java.util.Map;

public class IuPSSourcing extends Sourcing {
    public IuPSSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

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

        mapValues.put(0, "Attach");
        mapValues.put(1, "MS_Initiated_Detach");
        mapValues.put(2, "NW_Initiated_GPRS_Detach");
        mapValues.put(3, "Routing_Area_Update");
        mapValues.put(4, "MS_Requested_PDP_Context_Activation");
        mapValues.put(5, "NW_Requested_PDP_Context_Activation");
        mapValues.put(6, "MS_Requested_Secondary_PDP_Context_Activation");
        mapValues.put(7, "NW_Requested_Secondary_PDP_Context_Activation");
        mapValues.put(8, "MS_Initiated_PDP_Context_Modification");
        mapValues.put(9, "NW_Initiated_PDP_Context_Modification");
        mapValues.put(10, "MS_Initiated_PDP_Context_Deactivation");
        mapValues.put(11, "NW_initiated_PDP_Context_Deactivation");
        mapValues.put(12, "MS_Initiated_Service_Request");
        mapValues.put(13, "NW_Initiated_Service_Request");
        mapValues.put(14, "SRNS_Relocation");
        mapValues.put(17, "Gb_mode_GPRS_Paging");
        mapValues.put(18, "Handover");
        mapValues.put(21, "SMS_send");
        mapValues.put(22, "SMS_receive");
        mapValues.put(95, "Notification");
        mapValues.put(96, "SM_Status");
        mapValues.put(97, "GMM_Status");
        mapValues.put(98, "GMM_Information");
        mapValues.put(99, "Common_ID");
        mapValues.put(254, "Others");
        mappings.put("PROC_TYPE|PROC_TYPE", mapValues);

        mapValues = new HashMap<>();
        mapValues.put(1, "UTRAN");
        mapValues.put(2, "GERAN");
        mappings.put("RAT|RAT", mapValues);

        return mappings;
    }

    private Column convertUnixToTimestamp(Column column, boolean withMilliseconds) {
        return to_timestamp(from_unixtime(column), "yyyy-MM-dd HH:mm:ss" + (withMilliseconds? ".SSS" : ""));
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> ds) {
        Dataset<Row> locationRef = references.get("locationRef");

        locationRef = locationRef
                .select("CGISAI"
                        , "CELL_NAME", "SITE_NAME", "XPOS", "YPOS", "DIRECTIONANGLE", "LAYER4NAME", "LAYER3NAME"
                        , "LAYER1NAME")
                .withColumnRenamed("CELL_NAME", "CellName")
                .withColumnRenamed("SITE_NAME", "SiteName")
                .withColumnRenamed("XPOS", "Cell_lat")
                .withColumnRenamed("YPOS", "Cell_lon")
                .withColumnRenamed("DIRECTIONANGLE", "CellAngle")
                .withColumnRenamed("LAYER4NAME", "CityName")
                .withColumnRenamed("LAYER3NAME", "ProvinceName")
                .withColumnRenamed("LAYER1NAME", "RegionName"); // I kept RegionName since S1MME has it
        locationRef.persist();

        ds = ds.withColumn("RAI_TAI"
                        , concat(expr("substring(RAI_TAI,1,5)"), lit("_"), expr("conv(substring(RAI_TAI,6,4),16,10)")
                                , lit("_"), expr("conv(substring(RAI_TAI,-2),16,10)")))
                .withColumn("CellID"
                        , concat(expr("substring(SAI_CGI_ECGI,1,5)"), lit("_")
                                , expr("conv(substring(SAI_CGI_ECGI,6,4),16,10)"), lit("_")
                                , expr("conv(substring(SAI_CGI_ECGI,-4),16,10)")));

        ds = ds.withColumn("RECORD_DATE", convertUnixToTimestamp(col("PROC_STARTTIME"), false));
        ds = addTimeColumns(ds);

        HashMap<String, HashMap<Integer, String>> mappings = getOneToOneMappings();
        ds = applyOneToOneMappings(ds, mappings);

        String[] cols = new String[]{
                "RECORD_DATE", "RECORD_DATE_TEHRAN", "day",
                "SID", "IMSI", "MSISDN", "RAT", "RAI_TAI", "CellID", "PROC_TYPE",
                "PROC_STARTTIME",
                "CellName", "SiteName", "Cell_lat", "Cell_lon", "CellAngle",
                "CityName", "ProvinceName", "RegionName", "partition", "offset"
        };

        Column locationCond = ds.col("SAI_CGI_ECGI").equalTo(col("loc_ref.CGISAI"));

        ds = ds.join(broadcast(locationRef.as("loc_ref")), locationCond, "leftouter")
                .selectExpr(cols)
                .withColumnRenamed("CellID", "SAI_CGI_ECGI");

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
