package ir.mohaymen.sqm.utils.security.vault;

import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.connectors.VaultSource;
import ir.mohaymen.sqm.exceptions.ForbiddenException;
import ir.mohaymen.sqm.utils.ConfigService;
import ir.mohaymen.sqm.utils.SparkSessionHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class VaultManager {
    private VaultClient client;
    private KeyStore keyStore;
    private VaultManager(KeyStore keyStore ,VaultClient client){
        this.keyStore = keyStore;
        this.client = client;
    }
    private static VaultManager instance;
    private final int MAX_RETRIES = 4;
    public static synchronized VaultManager getInstance() throws Exception {
        if (instance == null){
            String baseAddress = ConfigService.getInstance().getOptionsByType("vault").get("baseAddress");
            String token = System.getenv("SPARK_VAULT_TOKEN");
            VaultClient vaultClient = new VaultClient(baseAddress ,token);
            KeyStore store = new KeyStore();
            instance =  new VaultManager(store ,vaultClient);
        }
        return instance;
    }

    public void initDefaults() throws Exception {
        KeyModel defaullKey = requestWithRetry("default");
        keyStore.setDefaultKey(defaullKey);
    }
    public KeyModel getDefaultKey(){
        return keyStore.getDefaultKey();
    }

    public List<KeyModel> getKeys(LocalDate currentDate , int watermark) throws Exception {
        List<KeyModel> keys = new ArrayList<>();
        boolean needsRefresh = false;
        for (int i = 0 ; i < watermark ; i++){
            String date = currentDate.minusDays(i).toString();
            KeyModel key = keyStore.get(date);
            if (key == null){
                key = requestWithRetry(date);
                needsRefresh = true;
            }
            keys.add(key);
        }
        if (needsRefresh){
            resetKeys(keys);
        }
        return keys;
    }

    public Dataset<Row> readKeys(SparkSession session) throws Exception {
        VaultSource vaultSource = new VaultSource(session);
        return vaultSource.read();
    }
    private void resetKeys(List<KeyModel> keys){
        keyStore.refresh(keys);
    }


    KeyModel requestWithRetry(String date) throws Exception {
        Exception lastException = null;
        for (int i = 0 ; i < MAX_RETRIES ; i++) {
            try {
                return new KeyModel(
                        date,
                        client.exportKey("/v1/transit/export/encryption-key/", date),
                        client.exportKey("/v1/transit/export/hmac-key/", date)
                        , Objects.equals(date, "default"));
            }
            catch (Exception e){
                  lastException = e;
                  handleExceptions(e);
            }
        }
        throw new Exception("Could not get key and salt from vault after " + String.valueOf(MAX_RETRIES) +" retries \n" + lastException.toString());
    }

    private void handleExceptions(Exception e) throws Exception {
        if (e instanceof ForbiddenException){
            client.setToken(System.getenv("SPARK_VAULT_TOKEN"));
        }
        else throw e;
    }
}
