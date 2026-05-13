package ir.mohaymen.sqm.connectors;

import ir.mohaymen.sqm.utils.ConfigService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

 abstract public class Connector {

     public Connector() {
         setOptions();
     }

     Map<String, String> options;
     protected SparkSession session;

     protected void setOptions(){
         this.options = ConfigService.getInstance().getOptionsByType(this.getClass().getSuperclass().getSimpleName().toLowerCase());
     }


 }
