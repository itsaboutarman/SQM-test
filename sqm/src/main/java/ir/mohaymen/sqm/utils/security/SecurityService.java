package ir.mohaymen.sqm.utils.security;

import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.connectors.VaultSource;
import ir.mohaymen.sqm.utils.security.vault.VaultManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class SecurityService {
    SparkSession session;
    VaultManager vaultManager;
    boolean keysAdded = false;

    public SecurityService(SparkSession session) throws Exception {
        this.session = session;
        vaultManager = VaultManager.getInstance();
    }
    public Dataset<Row> addSecurityKeys(Dataset<Row> ds) throws Exception {
        vaultManager.initDefaults();
        // TODO: better to do not use VaultManager inside this method
        Dataset<Row> keys = vaultManager.readKeys(session);
        ds = ds.join(broadcast(keys).alias("keyRef"),
                (keys.col("date_enc").equalTo(date_format(ds.col("RECORD_DATE"),"yyyy-MM-dd"))),
                "leftouter");
        ds = getOrDefault(ds, "key_enc",lit(vaultManager.getDefaultKey().encKey));
        ds = getOrDefault(ds, "salt", lit(vaultManager.getDefaultKey().salt));
        ds = getOrDefault(ds, "is_default", lit(true));
        // TODO: better to use coalesce() instead of getOrDefault()
        ds = ds.drop("date_enc");
        keysAdded = true;
        return ds;
    }

    public Dataset<Row> encrypt(Dataset<Row> ds , String[] columns ,String encColName) throws Exception {
        if (columns.length == 0){
            return ds;
       }
        if (!keysAdded){
            ds = addSecurityKeys(ds);
        }
        ds = ds.withColumn("key_enc",lit("4ufCC8EhBIv1E6SYbdHrc7v/eRDyObuVDgikMY7LbJQ="))
        .withColumn("salt",lit("DstPusIx0MCJo4n/EYVMbrX+NgphrUG8rd8a37XphVk="));
        String nvlExpr = Arrays.stream(columns).map(
                i -> "nvl(cast ("+i+" as string),'')"
        ).collect(joining(", ", "concat_ws('|'," , ")"));
        ds = ds.withColumn(encColName, base64(aes_encrypt(expr(nvlExpr),
                unbase64(col("key_enc")),
                lit("GCM"))));
        
        return ds;
    }
    public Dataset<Row> hash(Dataset<Row> ds , String[] columns  , String prefix , String postfix) throws Exception {
        if (!keysAdded){
           ds = addSecurityKeys(ds);
       }
        for (String column : columns) {
            ds = ds.withColumn(prefix + column + postfix, sha2(concat( ds.col(column),col("salt")), 256));
        }
        for (String column: columns) {
            ds = ds.drop(column);
        }
        ds = ds.drop("key_enc","salt");
        return ds;
    }

    private Dataset<Row> getOrDefault(Dataset<Row> ds, String colName, Column defaultValue) {
        return ds.withColumn(colName ,when(col("is_default").isNull(),
                defaultValue).otherwise(col(colName)));
    }
}
