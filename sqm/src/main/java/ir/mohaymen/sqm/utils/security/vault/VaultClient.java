package ir.mohaymen.sqm.utils.security.vault;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.mohaymen.sqm.exceptions.ForbiddenException;
import javassist.NotFoundException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

public class VaultClient {
    private CloseableHttpClient client;
    private String baseAddress;
    private String token;
    public VaultClient(String baseAddress , String token) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain ,String authType) -> true;
        SSLContext sslContext =  SSLContexts.custom()
                .loadTrustMaterial(null,acceptingTrustStrategy)
                .build();

        this.client = HttpClients.custom().setSSLContext(sslContext)
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();
        this.baseAddress = baseAddress;
        this.token = token;
    }
    public VaultClient(String baseAddress){
        this.client = HttpClients.createDefault();
        this.baseAddress = baseAddress;
    }

    public void setToken(String token){
        this.token = token;
    }
    public String exportKey(String path,String date) throws Exception {
        String value = "";
        HttpGet request = new HttpGet(baseAddress + path + date);
        request.addHeader("X-Vault-Token", token);
        try (CloseableHttpResponse response = client.execute(request)) {
            if (response.getStatusLine().getStatusCode() == 200){
            String responseBody = EntityUtils.toString(response.getEntity());
            ObjectMapper mapper = new ObjectMapper();
            value = mapper.readTree(responseBody)
                    .path("data")
                    .path("keys")
                    .path("1")
                    .asText();
            } else if (response.getStatusLine().getStatusCode() == 403) {
                throw new ForbiddenException();
            } else {
                throw new Exception();
            }
        }
        if (value.isEmpty()){
            throw new NotFoundException("Key for date: " + date +"not found!.");
        }
        return value;
    }

    //unused for now but might be used later
    public String login(String path ,String userName , String password) throws Exception {
        String value = "";
        HttpPost request = new HttpPost(baseAddress + path + userName);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("password", password);
        ObjectMapper requestMapper = new ObjectMapper();
        String json = requestMapper.writeValueAsString(jsonMap);
        StringEntity entity = new StringEntity(json);
        request.setEntity(entity);
        try (CloseableHttpResponse response = client.execute(request)) {
            if (response.getStatusLine().getStatusCode() == 200){
                String responseBody = EntityUtils.toString(response.getEntity());
                ObjectMapper mapper = new ObjectMapper();
                value = mapper.readTree(responseBody)
                        .path("auth")
                        .path("client_token")
                        .asText();
            }
            else {
                System.out.println(response.getEntity().toString());
                throw new Exception("Could not login to vault. StatusCode :" + response.getStatusLine().getStatusCode() );
            }
        }
        if (value.isEmpty()){
            throw new NotFoundException("Vault could not generate token for this user!!");
        }
        return value;
    }
}
