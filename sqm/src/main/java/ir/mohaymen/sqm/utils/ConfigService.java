package ir.mohaymen.sqm.utils;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;



// TODO: better to do not use this service inside other classes
public class ConfigService {
    private static final Logger logger = Logger.getLogger(ConfigService.class.getName());
    private static ConfigService instance;
    private Properties properties = new Properties();
    private Map<String, String> options = new HashMap<>();


    private ConfigService(String packageName) throws IOException {
        setProperties(getConfigPath(packageName));
        setOptions(properties);
    }

    public static ConfigService initialize(String packageName) throws IOException {
        if (instance == null) {
            instance = new ConfigService(packageName);
        } else {
            throw new IllegalStateException("ConfigService is already initialized");
        }
        return instance;
    }

    public static ConfigService getInstance() {
        if (instance == null) {
            throw new IllegalStateException("ConfigService not initialized");
        }
        return instance;
    }

    private void setProperties(String configPath) throws IOException {
        Path path = Paths.get(configPath);

        if (!Files.exists(path)) {
            logger.severe("Unable to find " + configPath);
            throw new IOException("Configuration file not found: " + configPath);
        }

        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
    }


    private String getConfigPath(String packageName) {
        return "/tmp/spark/app.properties";
    }

    private void setOptions(Properties properties) {
        properties.forEach((key, value) -> {options.put(key.toString(), value.toString());});
        if(options.containsKey("spark.scheduler.mode"))
            options.put("spark.scheduler.allocation.file", getResourceFilePath("scheduler-pools.xml"));
    }

    private static String getResourceFilePath(String fileName) {
        URL resource = ConfigService.class.getClassLoader().getResource(fileName);
        String path = "";
        if (resource.getProtocol().equals("jar")) {
            try {
                InputStream input = ConfigService.class.getClassLoader().getResourceAsStream(fileName);
                File file = File.createTempFile(fileName.split("\\.")[0], "."+fileName.split("\\.")[1]);
                OutputStream out = Files.newOutputStream(file.toPath());
                int read;
                byte[] bytes = new byte[1024];

                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
                out.close();
                file.deleteOnExit();
                path = "file://" + file.getAbsolutePath();
            } catch (IOException ex) {
                System.out.println(
                        String.format("Error encountered while creating a tmp file for resource %s: \n", resource.getPath())
                                + ex);
                System.exit(0);
            }
        } else {
            path = resource.getPath();
        }
        return path;
    }

    public Map<String, String> getOptions() {
        return this.options;}

    public Map<String, String> getOptionsByType(String type) {
        Map<String, String> selectedOptions = new HashMap<>();
        this.options.forEach((key, value) -> {
            if (key.startsWith(type)) {
                if (key.split("\\.", 2).length > 1) {
                    selectedOptions.put(key.split("\\.", 2)[1], value);
                }
            }
        });
        return selectedOptions;
    }
}
