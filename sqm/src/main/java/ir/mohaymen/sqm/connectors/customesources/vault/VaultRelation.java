package ir.mohaymen.sqm.connectors.customsources.vault;

import ir.mohaymen.sqm.utils.security.vault.KeyModel;
import ir.mohaymen.sqm.utils.security.vault.VaultManager;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

public class VaultRelation extends BaseRelation implements TableScan
{
    private final SQLContext sqlContext;
    private final scala.collection.immutable.Map<String, String> parameters;
    private final StructType schema;
    private final int watermark;
    public VaultRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String>  parameters) {
        this(sqlContext, parameters, inferSchema());
    }

    public VaultRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters, StructType schema) {
        this.sqlContext = sqlContext;
        this.parameters = parameters;
        this.schema = schema;
        watermark = Integer.parseInt(parameters.get("watermark").get());
    }
    @Override
    public SQLContext sqlContext() { return sqlContext; }

    @Override
    public StructType schema() { return schema; }

    @Override
    public RDD<Row> buildScan() {
        List<KeyModel> keys;
        try {
            keys = VaultManager.getInstance().getKeys(LocalDate.now(ZoneOffset.UTC),watermark);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<Row> rows = new ArrayList<>();
        for (KeyModel key : keys){
            rows.add(RowFactory.create(key.encKey ,key.salt ,key.name ,false));
        }
        return sqlContext.sparkContext().parallelize(JavaConverters.asScalaBuffer(rows).toSeq(), 1,
                ClassTag$.MODULE$.apply(Row.class)).toJavaRDD().rdd();
    }

    private static StructType inferSchema() {
        return new StructType(new StructField[]{
                new StructField("key_enc", DataTypes.StringType, false, Metadata.empty()),
                new StructField("salt", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date_enc", DataTypes.StringType, false, Metadata.empty()),
                new StructField("is_default", DataTypes.BooleanType, false, Metadata.empty()),
        });
    }
}






