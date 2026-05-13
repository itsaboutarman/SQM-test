package ir.mohaymen.sqm.utils.security.vault;

import java.util.HashMap;
import java.util.List;

public class KeyStore {
private HashMap<String, KeyModel> keys = new HashMap<>();
private KeyModel defaultKey;
public void setDefaultKey(KeyModel keyModel){
    defaultKey = keyModel;
}
public KeyModel getDefaultKey(){
    return defaultKey;
}

public void put(String name , String key_enc , String salt ,boolean isDefault){
   keys.put(name , new KeyModel(name , key_enc , salt ,isDefault));
}
public void put(String name , String key_enc , String salt){
        keys.put(name , new KeyModel(name,key_enc , salt));
    }
public void refresh(List<KeyModel> newKeys){
    keys.clear();
    for (KeyModel model : newKeys){
        keys.put(model.name , model);
    }
}

public KeyModel get(String name){
    return keys.getOrDefault(name , null);
}
}
