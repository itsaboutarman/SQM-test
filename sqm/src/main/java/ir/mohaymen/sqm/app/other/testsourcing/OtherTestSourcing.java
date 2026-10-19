package ir.mohaymen.sqm.app.other.testsourcing;

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

public class OtherTestSourcing extends Sourcing{
    public OtherTestSourcing(Source source, Sink sink, Map<String, Dataset<Row>> references) {
        super(source, sink, references);
    }

    @Override
    protected Dataset<Row> extractColumns(Dataset<Row> ds) {
        return FormatConverter.fromCSV(ds, Schema.getSchema(), "|");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", from_unixtime(col("BEGIN_TIME")))
                .withColumn("RECORD_DATE", to_timestamp(col("RECORD_DATE"), "yyyy-MM-dd HH:mm:ss"));
        ds = addTimeColumns(ds);
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
        // Column filteredParts_old = expr(
        //         "filter(" +
        //                 "split(" + cleanAPN.expr().sql() + ", '\\\\.'), " +
        //                 "x -> length(x) > 1" +
        //                 ")"
        // );
        Column filteredParts =
                filter(
                        split(cleanAPN, "\\."),
                        x -> length(x).gt(lit(1))
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
