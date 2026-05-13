// File: src/main/java/com/example/httpapi/DefaultSource.java

package ir.mohaymen.sqm.connectors.customsources.vault;

import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;



public class VaultSource implements RelationProvider, SchemaRelationProvider, DataSourceRegister {
    @Override
    public String shortName() {
        return "VaultSource";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters) {
        return new VaultRelation(sqlContext, parameters);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters, StructType schema) {
                return new VaultRelation(sqlContext, parameters, schema);
    }
}

