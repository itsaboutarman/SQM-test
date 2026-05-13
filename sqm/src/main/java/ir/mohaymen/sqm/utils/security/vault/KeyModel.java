package ir.mohaymen.sqm.utils.security.vault;

public class KeyModel {
    public String encKey;
    public String salt;
    public String name;
    public boolean isDefault;
    public KeyModel(String name , String encKey , String salt , boolean isDefault){
        this.name = name;
        this.encKey = encKey;
        this.salt = salt;
        this.isDefault = isDefault;
    }

    public KeyModel(String name , String encKey , String salt){
        this.name = name;
        this.encKey = encKey;
        this.salt = salt;
    }
}
