package ir.mohaymen.sqm.connectors;

import ir.mohaymen.sqm.utils.ConfigService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VaultSource extends Source{
    public VaultSource(SparkSession session) throws Exception {
        super(session);
    }
    @Override
    public Dataset<Row> read() {
          return session
                .read()
                .format("ir.mohaymen.sqm.connectors.customsources.vault.VaultSource")
                .options(ConfigService.getInstance().getOptionsByType("vault"))
                .load();
    }
}
