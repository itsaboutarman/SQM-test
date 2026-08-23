package ir.mohaymen.sqm;

// TODO: Suggested structure for packages :
//  app/
//  |    spark/
//  |    flink/
//  |    App.java
//  |    AppBuilder.java
//  |    AppLayer.java
//  |    Layer.java
//  connectors/
//  ...
//  Main.java
import ir.mohaymen.sqm.app.*;
import ir.mohaymen.sqm.connectors.ConnectorAnnotation;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import ir.mohaymen.sqm.exceptions.InstantiationException;
import ir.mohaymen.sqm.exporters.KeepThriftUpdatedListener;
import ir.mohaymen.sqm.exporters.StatsListener;
import ir.mohaymen.sqm.lake.LakeManager;
import ir.mohaymen.sqm.utils.ConfigService;
import ir.mohaymen.sqm.utils.SparkSessionHandler;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.reflections.Reflections;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;

public class Main {
    private static final String projectPackage = "ir.mohaymen.sqm";
    private static ConfigService configService;

    public static void main(String[] args) throws Exception {

        String dataFlow = System.getenv("CLASS_NAME");
        //String dataFlow = "IMSourcing";
        String appPackageName = getPackage(Job.class, dataFlow);
        configService = ConfigService.initialize(appPackageName);
        SparkSession session = SparkSessionHandler.getSession();
        App app = new AppBuilder(session , configService ,appPackageName)
                .withSource(getPackage(ConnectorAnnotation.class, capitalize(configService.getOptions().get("source")) + "Source"))
                .withSink(getPackage(ConnectorAnnotation.class, capitalize(configService.getOptions().get("sink")) + "Sink"))
                .loadReferences()
                .addLayers()
                .build();

        // waiting for initialize lake manager and set retention properties in delta lake:
        if (app instanceof Sourcing || app instanceof Lake){
            LakeManager.initialize();
        }
        else{
            Logger.getLogger(Main.class.getName()).info("[Lake Manager] Lake Manager not initialized.");
        }


        StatsListener exporter = new StatsListener(configService.getOptions());
        session.streams().addListener(exporter);
        // TODO: Better to be in APP
        StreamingQuery query = app.run();
        Logger.getLogger(Main.class.getName()).info(
                String.format("[Lake Manager] Is app Sourcing or Lake? %b", app instanceof Sourcing || app instanceof Lake));

        if (KeepThriftUpdatedListener.shouldKeepThriftUpdated(app, configService.getOptions())){
            KeepThriftUpdatedListener keepThriftUpdatedListener= new KeepThriftUpdatedListener(session, configService.getOptions());
            session.streams().addListener(keepThriftUpdatedListener);
        }
        query.awaitTermination();
    }

    private static String getPackage(Class<? extends Annotation> annotationClass, String className) {
        Reflections reflections = new Reflections(projectPackage);
        HashMap<String, String> classes = reflections.getTypesAnnotatedWith(annotationClass).stream()
                .collect(Collectors.toMap(
                        Class::getSimpleName,
                        Class::getName,
                        (existing, replacement) -> existing,
                        HashMap::new
                ));
        return classes.getOrDefault(className, null);
    }


    public static String capitalize(String str) {
        if (str == null || str.isEmpty()) return str;
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }
}




