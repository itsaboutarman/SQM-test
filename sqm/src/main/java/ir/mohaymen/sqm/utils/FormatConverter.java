package ir.mohaymen.sqm.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class FormatConverter {

    public static Dataset<Row> fromCSV(Dataset<Row> ds, StructType schema, String separator) {
        java.util.HashMap<String, String> optionsMap = new java.util.HashMap<String, String>();
        optionsMap.put("sep", separator);
        String[] otherColumns = ArrayUtils.removeElement(ds.schema().fieldNames(), "value");
        return ds.withColumn("newColumn", functions.from_csv(functions.col("value")
                        , functions.lit(schema.toDDL()), optionsMap))
                .select("newColumn.*", otherColumns);
    }
}
