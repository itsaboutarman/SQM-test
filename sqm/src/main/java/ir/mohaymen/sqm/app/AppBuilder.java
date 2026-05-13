package ir.mohaymen.sqm.app;

import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.exceptions.InstantiationException;
import ir.mohaymen.sqm.utils.ConfigService;
import ir.mohaymen.sqm.utils.ReferenceLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AppBuilder {
    private Sink sink;
    private Source source;
    private SparkSession session;
    private App app;
    private Map<String, Dataset<Row>> references;
    private List<Class<?>> layers = new ArrayList<>();
    private String appPackageName;
    private ConfigService config;
    public AppBuilder(SparkSession session , ConfigService config, String appPackageName){
        this.session = session;
        this.appPackageName = appPackageName;
        this.config = config;
    }

    public AppBuilder withSource(String packageName) throws InstantiationException {
        source = getSourceConnector(packageName,session);
        return this;
    }
    public AppBuilder withSink(String packageName) throws InstantiationException {
        this.sink = getSinkConnector(packageName);
        return this;
    }
    public AppBuilder loadReferences(){
        references = new ReferenceLoader(session,config.getOptionsByType("reference"))
                .loadReferences();
        return this;
    }


    public AppBuilder addLayers() throws NoSuchMethodException, InvocationTargetException, java.lang.InstantiationException, IllegalAccessException {
        Reflections reflection = new Reflections();
        Set<Class<? extends  AppLayer>> appLayerSet = reflection.getSubTypesOf(AppLayer.class);
        Map<String,String> layerConf = config.getOptionsByType("layer.name");
        for (int i = 1 ; i <= layerConf.size() ; i++){
            String name = layerConf.get("name." + String.valueOf(i));
            Class<? extends  AppLayer> providerClass = appLayerSet.stream().filter( c -> c.getDeclaredAnnotation(Layer.class).name().equals(name))
                    .findFirst()
                    .get();
            layers.add(providerClass);
        }
        return this;
    }

    public App build() throws Exception {
        if (app != null){
            return app;
        }
        if (source == null || sink == null){
            throw new IllegalArgumentException("source and sink should not be null!!");
        }
        app = instantiateAppObject(source ,sink ,appPackageName ,references);
        for (Class<?> layer : layers){
            app = (App) layer.getDeclaredConstructor(App.class ,SparkSession.class, Map.class)
                    .newInstance(app ,session , config.getOptionsByType(layer.getDeclaredAnnotation(Layer.class).name()));
        }
        return app;
    }

    private App instantiateAppObject(Source source, Sink sink, String packageName, Map<String, Dataset<Row>> references) throws ir.mohaymen.sqm.exceptions.InstantiationException {
        try {
            Class<?> sourceClass = Class.forName(packageName);
            Constructor<?> SourceConstructor = sourceClass.getDeclaredConstructor(Source.class, Sink.class, Map.class);
            return  (SparkApp) SourceConstructor.newInstance(source, sink, references);
        } catch (ClassNotFoundException | NoSuchMethodException | java.lang.InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new InstantiationException("Error instantiating session object for package: " + packageName, e);
        }
    }
    private static Source getSourceConnector(String packageName, SparkSession session) throws InstantiationException {
        try {
            Class<?> sourceClass = Class.forName(packageName);
            Constructor<?> SourceConstructor = sourceClass.getDeclaredConstructor(SparkSession.class);
            return (Source) SourceConstructor.newInstance(session);
        } catch (ClassNotFoundException | NoSuchMethodException | java.lang.InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new InstantiationException("Error instantiating source object for package: " + packageName, e);
        }
    }

    private static Sink getSinkConnector(String packageName) throws InstantiationException {
        try {
            Class<?> sourceClass = Class.forName(packageName);
            Constructor<?> SourceConstructor = sourceClass.getDeclaredConstructor();
            return (Sink) SourceConstructor.newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | java.lang.InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new InstantiationException("Error instantiating sink object for package: " + packageName, e);
        }
    }
}
