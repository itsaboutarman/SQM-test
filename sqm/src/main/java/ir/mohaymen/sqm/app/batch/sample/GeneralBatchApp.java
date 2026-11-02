package ir.mohaymen.sqm.app.batch.sample;
import ir.mohaymen.sqm.app.BatchApp;
import ir.mohaymen.sqm.app.BatchAppResult;
import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.utils.ConfigService;
import ir.mohaymen.sqm.utils.FormatConverter;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.spark.sql.functions.lit;


@Job
public class GeneralBatchApp extends BatchApp {
    public GeneralBatchApp(SparkSession session, ConfigService config) {
        super(session, config);
    }

    @Override
    public BatchAppResult run() throws Exception {
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-11' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-12' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-13' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-14' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-15' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-16' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-17' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-18' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-19' ");
        session.sql("delete from delta.`s3a://gtpv2/source/` where day='2026-10-20' ");
        session.sql("VACUUM delta.`s3a://gtpv2/source/` retain 6 hours");
        session.sql("VACUUM delta.`s3a://s6a/source/` retain 4 hours");
        return new BatchAppResult();
    }

    private boolean write(Dataset<Row> data){
        data = toJson(data);
        data.write()
                .format("kafka")
                .options(config.getOptionsByType("sink"))
                .save();
        return true;
    }

    private Dataset<Row> toJson(Dataset<Row> ds) {
        return ds.select(functions.to_json(functions.struct("*")).alias("value"))
                .selectExpr("CAST(value AS STRING)");
    }

    private StructType getSchema(String schemaclassName) throws Exception {
        try {
            Class<?> schemaClass = Class.forName(schemaclassName);
            Method getSchemaMethod = schemaClass.getMethod("getSchema");
            StructType schema = (StructType) getSchemaMethod.invoke(null);
            return schema;
        }
        catch (Exception e){
            throw  new Exception(e.getMessage());
        }
    }
}