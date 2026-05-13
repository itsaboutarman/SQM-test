package ir.mohaymen.sqm.app.email.summary.emailsummary;

import ir.mohaymen.sqm.app.Job;
import ir.mohaymen.sqm.app.Summary;
import ir.mohaymen.sqm.connectors.Sink;
import ir.mohaymen.sqm.connectors.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
@Job
public class EmailSummary extends Summary {
    public EmailSummary(Source source, Sink sink, Map<String, Dataset<Row>> reference) {
        super(source, sink, reference);
    }

    @Override
    protected Dataset<Row> selectColumns(Dataset<Row> ds) {
        return ds.select("RECORD_DATE","Country","Region","Province","City","SGW","PGW","SGSN",
                "GGSN","SGSN_POOL","RAT","FST_TRANS_FLAG","EMAIL_PROT_TYPE","TRANS_TYPE","L7_UL_GOODPUT_FULL_MSS",
                "DATATRANS_UL_DURATION" , "DATATRANS_DW_DURATION" ,"L7_DW_GOODPUT_FULL_MSS");
    }

    @Override
    protected Dataset<Row> analyze(Dataset<Row> ds) {
        ds = ds.withColumn("RECORD_DATE", window(col("RECORD_DATE"), "5 minutes").getField("start"));
        ds = ds.where(col("FST_TRANS_FLAG").isNotNull())
                .withColumn("SMTP_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(2)
                        .and(col("TRANS_TYPE").equalTo(15)) ,1).otherwise(0))
                 .withColumn("SMTP_Successful_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(2)
                 .and(col("FST_TRANS_FLAG").equalTo(0))
                .and(col("TRANS_TYPE").equalTo(15)) ,1).otherwise(0))
                .withColumn("SMTP_Send_Size" ,when(col("EMAIL_PROT_TYPE").equalTo(2)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(4)) ,col("L7_UL_GOODPUT_FULL_MSS")).otherwise(0))
                .withColumn("SMTP_Send_Duration" ,when(col("EMAIL_PROT_TYPE").equalTo(2)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(4)) ,col("DATATRANS_UL_DURATION")).otherwise(0))
                .withColumn("POP3_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(1)
                        .and(col("TRANS_TYPE").equalTo(2)) ,1).otherwise(0))
                .withColumn("POP3_Successful_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(1)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(2)) ,1).otherwise(0))
                .withColumn("POP3_Download_Size" ,when(col("EMAIL_PROT_TYPE").equalTo(1)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(6)) ,col("L7_DW_GOODPUT_FULL_MSS")).otherwise(0))
                .withColumn("POP3_Download_Duration" ,when(col("EMAIL_PROT_TYPE").equalTo(1)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(6)) ,col("DATATRANS_DW_DURATION")).otherwise(0))
                .withColumn("IMAP_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(1)) ,1).otherwise(0))
                .withColumn("IMAP_Successful_Logins" ,when(col("EMAIL_PROT_TYPE").equalTo(0)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").equalTo(1)) ,1).otherwise(0))
                .withColumn("IMAP_Download_Size" ,when(col("EMAIL_PROT_TYPE").equalTo(0)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").isin(7,12)) ,col("L7_DW_GOODPUT_FULL_MSS")).otherwise(0))
                .withColumn("IMAP_Download_Duration" ,when(col("EMAIL_PROT_TYPE").equalTo(0)
                        .and(col("FST_TRANS_FLAG").equalTo(0))
                        .and(col("TRANS_TYPE").isin(7,12)) ,col("DATATRANS_DW_DURATION")).otherwise(0))
                .withColumn("Webmail_Logins" ,when(col("TRANS_TYPE").equalTo(32) ,1).otherwise(0))
                .withColumn("Webmail_Successful_Logins" ,when(col("FST_TRANS_FLAG").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(32)) ,1).otherwise(0))
                .withColumn("Webmail_Download_Size" ,when(col("FST_TRANS_FLAG").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(35)) ,col("L7_DW_GOODPUT_FULL_MSS")).otherwise(0))
                .withColumn("Webmail_Download_Duration" ,when(col("FST_TRANS_FLAG").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(35)) ,col("DATATRANS_DW_DURATION")).otherwise(0))
                .withColumn("Webmail_Send_Size" ,when(col("FST_TRANS_FLAG").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(34)) ,col("L7_UL_GOODPUT_FULL_MSS")).otherwise(0))
                .withColumn("Webmail_Send_Duration" ,when(col("FST_TRANS_FLAG").equalTo(0)
                        .and(col("TRANS_TYPE").equalTo(34)) ,col("DATATRANS_UL_DURATION")).otherwise(0));

        ds = aggregateFunc(ds);
        ds = generateKeyHash(ds , new String[]{
                "Country","Region","Province","City","SGW","PGW","SGSN",
                "GGSN","SGSN_POOL","RAT"
        });
        System.out.println("output schema:");
        ds.printSchema();
        return ds;
    }

    private Dataset<Row> aggregateFunc(Dataset<Row> ds){
        return ds.withWatermark("RECORD_DATE", "1440 minutes")
                .groupBy("RECORD_DATE","Country","Region","Province","City","SGW","PGW","SGSN",
                        "GGSN","SGSN_POOL","RAT")
                .agg(
                        sum("SMTP_Logins").alias("SMTP_Logins_Count"),
                        sum("SMTP_Successful_Logins").alias("SMTP_Successful_Logins_Count"),
                        sum("SMTP_Send_Size").alias("SMTP_Send_Size"),
                        sum("SMTP_Send_Duration").alias("SMTP_Send_Duration"),
                        sum("POP3_Logins").alias("POP3_Logins_Count"),
                        sum("POP3_Successful_Logins").alias("POP3_Successful_Logins_Count"),
                        sum("POP3_Download_Size").alias("POP3_Download_Size"),
                        sum("POP3_Download_Duration").alias("POP3_Download_Duration"),
                        sum("IMAP_Logins").alias("IMAP_Logins_Count"),
                        sum("IMAP_Successful_Logins").alias("IMAP_Successful_Logins_Count"),
                        sum("IMAP_Download_Size").alias("IMAP_Download_Size"),
                        sum("IMAP_Download_Duration").alias("IMAP_Download_Duration"),
                        sum("Webmail_Logins").alias("Webmail_Logins_Count"),
                        sum("Webmail_Successful_Logins").alias("Webmail_Successful_Logins_Count"),
                        sum("Webmail_Download_Size").alias("Webmail_Download_Size"),
                        sum("Webmail_Download_Duration").alias("Webmail_Download_Duration"),
                        sum("Webmail_Send_Size").alias("Webmail_Send_Size"),
                        sum("Webmail_Send_Duration").alias("Webmail_Send_Duration")
                );
    }
}
