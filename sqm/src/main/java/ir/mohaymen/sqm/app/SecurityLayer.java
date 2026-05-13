package ir.mohaymen.sqm.app;
import ir.mohaymen.sqm.utils.security.SecurityService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;
@Layer(name = "security")
public class SecurityLayer extends AppLayer{
    SecurityService securityService;
    Map<String,String> config;
    SparkSession session;
    public SecurityLayer(App app , SparkSession session, Map<String,String> config) throws Exception {
        super(app);
        this.session = session;
        this.config = config;
        this.securityService = new SecurityService(session);
    }

    @Override
    public Dataset<Row> process(Dataset<Row> ds) throws Exception {
        if (app instanceof Sourcing){
            ds = app.process(ds);
            String[] securityColumns = getSecurityColumns();
            ds = securityService.encrypt(ds ,securityColumns ,"encrypted");
            ds = securityService.hash(ds ,securityColumns ,"","_hash");
        }
        return ds;
    }

    private String[] getSecurityColumns() throws Exception {
        String columnsStr = config.getOrDefault("columns","");
        if(columnsStr.isEmpty()){
            throw new Exception("Security columns can not be empty when security layer added.");
        }
        return columnsStr.split(",");
    };
}



