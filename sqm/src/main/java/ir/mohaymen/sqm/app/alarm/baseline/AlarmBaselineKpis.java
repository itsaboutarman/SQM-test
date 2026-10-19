package ir.mohaymen.sqm.app.alarm.baseline;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public final class AlarmBaselineKpis {

    private AlarmBaselineKpis() {
    }

    public static final String KPI_NAME = "kpi_name";

    /** Columns of the result of allKpis(), in this order. */
    public static final String[] KPI_COLUMNS = {
            "RECORD_DATE", "Access_Type", "Country", "Region", "Province", "City", KPI_NAME, "kpi_num", "kpi_denom"};

    /** The columns of each source table that the calculation uses. Only these are read from StarRocks. */
    public static final String REGISTER_COLUMNS = "RECORD_DATE,Country,Region,Province,City,Access_Type,Registration_Type,"
            + "Source_NE_Type,Interface_Type,SuccessCount,TotalCount";
    public static final String CALL_COLUMNS = "RECORD_DATE,Country,Region,Province,City,Access_Type,Calling_or_Called_Party_Indicator,"
            + "Source_NE_Type,Destination_NE_Type,Interface_Type,Service_Type,Service_Status,Release_Cause_Code,SuccessCount,TotalCount";
    public static final String TCP_COLUMNS = "RECORD_DATE,Country,Region,Province,City,RAT,TCP_Connections_Status,Downlink_RTT_Long_Numbers,"
            + "Downlink_RTT_Times,TCP_Downlink_Retransmitted_Packets_with_Payload,TCP_Downlink_Packets_With_Payload,Downlink_Traffic_Bytes,"
            + "TCP_Connection_Success_Count,Total_TCP_Connection_Count";
    /** The columns of the result table that are needed from the day before (num_d7 and denom_d7 are only written). */
    public static final String BASELINE_COLUMNS = "RECORD_DATE,Access_Type,Country,Region,Province,City,kpi_name,kpi_num,kpi_denom,"
            + "num_d1,num_d2,num_d3,num_d4,num_d5,num_d6,denom_d1,denom_d2,denom_d3,denom_d4,denom_d5,denom_d6";

    /**
     * The same filter as hasCountry(), as SQL for the WHERE of the StarRocks query (push down): StarRocks does not send
     * the rows without a country. hasCountry() is also done in Spark, so the result is the same without it.
     */
    public static final String COUNTRY_FILTER_SQL = "Country IS NOT NULL AND Country <> ''";

    /** The register and call summaries have 5 minute buckets. The calculation uses buckets of 15 minutes (time_slice in the query). */
    private static final long SLOT_SECONDS = 15 * 60;

    /**
     * The columns of the key besides RECORD_DATE. StarRocks can not use all of them as a primary key (limit), so they are
     * put in one number, key_hash, and the primary key of the table is (RECORD_DATE, key_hash).
     */
    public static final String[] KEY_COLUMNS = {"Access_Type", "Country", "Region", "Province", "City", "kpi_name"};

    /** Release cause codes that the SQL counts as "network" failures of an MO call. */
    private static final Object[] NETWORK_RELEASE_CAUSES = {
            1, 403, 404, 405, 413, 414, 415, 416, 422, 423, 480, 484, 486, 487, 488, 600, 603, 604, 606};

    // ------------------------------------------------------------------------------------------------
    // Access type
    // ------------------------------------------------------------------------------------------------

    /** Access_Type code of the IMS tables -> 2G / 3G / 4G / 5G / Unknown. */
    static Column imsAccessType(Column code) {
        return when(code.equalTo(3), lit("2G"))
                .when(code.isin(4, 5, 6, 7, 8, 44, 46, 47), lit("3G"))
                .when(code.isin(1, 2, 9, 43, 45), lit("4G"))
                .when(code.isin(59, 60, 61, 62, 63, 64), lit("5G"))
                .otherwise(lit("Unknown"));
    }

    /** RAT code of the TCP table -> 2G / 3G / 4G / Unknown. */
    static Column tcpAccessType(Column rat) {
        return when(rat.equalTo(1), lit("3G"))
                .when(rat.equalTo(2), lit("2G"))
                .when(rat.equalTo(6), lit("4G"))
                .otherwise(lit("Unknown"));
    }

    /**
     * time_slice(RECORD_DATE, INTERVAL 15 MINUTE): the start of the 15 minutes of RECORD_DATE (10:00, 10:05 and 10:10 are 10:00).
     * StarRocks has time_slice, Spark does not, so it is made from the seconds.
     */
    private static Column timeSlot() {
        return timestamp_seconds(floor(unix_seconds(col("RECORD_DATE")).divide(SLOT_SECONDS)).multiply(SLOT_SECONDS));
    }

    /** WHERE Country != '' AND Country IS NOT NULL */
    private static Column hasCountry() {
        return col("Country").isNotNull().and(col("Country").notEqual(""));
    }

    /** SUM(CASE WHEN cond THEN value ELSE 0 END) */
    private static Column sumIf(Column cond, String value) {
        return sum(when(cond, col(value)).otherwise(lit(0)));
    }

    // ------------------------------------------------------------------------------------------------
    // 1) Register SIP overview
    // ------------------------------------------------------------------------------------------------

    public static Dataset<Row> registerKpis(Dataset<Row> reg) {
        Column type = col("Registration_Type");
        Column src = col("Source_NE_Type");
        Column itf = col("Interface_Type");

        Column ue = src.isin(4, 1).and(itf.equalTo(8));
        Column init = type.equalTo(0).and(ue);
        Column rereg = type.equalTo(1).and(ue);
        Column third = type.equalTo(3).and(src.equalTo(8)).and(itf.equalTo(10));
        Column dereg = type.equalTo(4).and(ue);

        Dataset<Row> agg = reg
                .filter(hasCountry())
                .groupBy(timeSlot().alias("RECORD_DATE"), col("Country"), col("Region"), col("Province"), col("City"),
                        imsAccessType(col("Access_Type")).alias("Access_Group"))
                .agg(sumIf(init, "SuccessCount").alias("init_succ"),
                        sumIf(init, "TotalCount").alias("init_total"),
                        sumIf(rereg, "SuccessCount").alias("rereg_succ"),
                        sumIf(rereg, "TotalCount").alias("rereg_total"),
                        sumIf(third, "SuccessCount").alias("third_succ"),
                        sumIf(third, "TotalCount").alias("third_total"),
                        sumIf(dereg, "SuccessCount").alias("dereg_succ"),
                        sumIf(dereg, "TotalCount").alias("dereg_total"));

        return explodeKpis(agg, new KpiList()
                .rate("VoLTE Initial Register Success Rate", "VoLTE Initial Register Success Count",
                        "VoLTE Initial Register Total Count", "init_succ", "init_total")
                .rate("VoLTE ReRegister Success Rate", "VoLTE ReRegister Success Count",
                        "VoLTE ReRegister Total Count", "rereg_succ", "rereg_total")
                .rate("VoLTE Third Party Register Success Rate", "VoLTE Third Party Register Success Count",
                        "VoLTE Third Party Register Total Count", "third_succ", "third_total")
                .rate("VoLTE UE Deregister Success Rate", "VoLTE UE Deregister Success Count",
                        "VoLTE UE Deregister Total Count", "dereg_succ", "dereg_total"));
    }

    // ------------------------------------------------------------------------------------------------
    // 2) Call SIP overview
    // ------------------------------------------------------------------------------------------------

    public static Dataset<Row> callKpis(Dataset<Row> call) {
        Column ind = col("Calling_or_Called_Party_Indicator");
        Column src = col("Source_NE_Type");
        Column dst = col("Destination_NE_Type");
        Column itf = col("Interface_Type");
        Column svc = col("Service_Type");
        Column status = col("Service_Status");
        Column cause = col("Release_Cause_Code");

        Column moLeg = ind.equalTo(0).and(src.isin(1, 4));      // call starts in the IMS (MO)
        Column mtLeg = ind.equalTo(1).and(dst.isin(1, 4));      // call ends in the IMS (MT)
        Column ok = status.equalTo(0);
        Column voice = svc.equalTo(0);
        Column video = svc.equalTo(1);
        Column voiceOrVideo = svc.isin(0, 1);

        Column moConn = moLeg.and(itf.equalTo(8)).and(voiceOrVideo);
        Column netMoExtra = moLeg.and(itf.equalTo(8)).and(status.equalTo(1)).and(cause.isin(NETWORK_RELEASE_CAUSES)).and(voiceOrVideo);
        Column mgcf = ind.equalTo(0).and(dst.equalTo(10)).and(voice);
        Column icscf = ind.equalTo(0).and(dst.equalTo(5)).and(voice);
        Column eitherLeg = moLeg.or(mtLeg);

        Dataset<Row> agg = call
                .filter(hasCountry())
                .groupBy(timeSlot().alias("RECORD_DATE"), col("Country"), col("Region"), col("Province"), col("City"),
                        imsAccessType(col("Access_Type")).alias("Access_Group"))
                .agg(sumIf(moConn, "SuccessCount").alias("mo_conn_succ"),
                        sumIf(moConn, "TotalCount").alias("mo_conn_total"),
                        sumIf(netMoExtra, "TotalCount").alias("net_mo_conn_extra"),

                        sumIf(mgcf, "SuccessCount").alias("mo_mgcf_succ"),
                        sumIf(mgcf, "TotalCount").alias("mo_mgcf_total"),

                        sumIf(icscf.and(ok), "SuccessCount").alias("mo_icscf_succ"),
                        sumIf(icscf, "TotalCount").alias("mo_icscf_total"),

                        sumIf(eitherLeg.and(ok).and(voice), "SuccessCount").alias("voice_conn_succ"),
                        sumIf(eitherLeg.and(voice), "TotalCount").alias("voice_conn_total"),

                        sumIf(eitherLeg.and(ok).and(video), "SuccessCount").alias("video_conn_succ"),
                        sumIf(eitherLeg.and(video), "TotalCount").alias("video_conn_total"),

                        sumIf(moLeg.and(ok).and(voice), "SuccessCount").alias("voice_mo_succ"),
                        sumIf(moLeg.and(voice), "TotalCount").alias("voice_mo_total"),

                        sumIf(moLeg.and(ok).and(video), "SuccessCount").alias("video_mo_succ"),
                        sumIf(moLeg.and(video), "TotalCount").alias("video_mo_total"),

                        sumIf(mtLeg.and(ok).and(voice), "SuccessCount").alias("voice_mt_succ"),
                        sumIf(mtLeg.and(voice), "TotalCount").alias("voice_mt_total"),

                        sumIf(mtLeg.and(ok).and(video), "SuccessCount").alias("video_mt_succ"),
                        sumIf(mtLeg.and(video), "TotalCount").alias("video_mt_total"),

                        sumIf(mtLeg.and(ok).and(voiceOrVideo), "SuccessCount").alias("ims_mt_succ"),
                        sumIf(mtLeg.and(voiceOrVideo), "TotalCount").alias("ims_mt_total"));

        return explodeKpis(agg, new KpiList()
                .rate("IMS MO Connection Rate", "IMS MO Connection Success Count", "IMS MO Connection Total Count",
                        "mo_conn_succ", "mo_conn_total")

                // "Network" MO connection: the success count also includes the network-side failures
                .kpi("IMS Network MO Connection Rate",
                        lit(100).multiply(col("mo_conn_succ").plus(col("net_mo_conn_extra"))), col("mo_conn_total"))
                .kpi("IMS Network MO Connection Success Count", col("mo_conn_succ").plus(col("net_mo_conn_extra")), lit(0))
                .kpi("IMS Network MO Connection Total Count", col("mo_conn_total"), lit(0))

                .rate("VoLTE MO to MGCF Connection Rate", "VoLTE MO to MGCF Connection Success Count",
                        "VoLTE MO to MGCF Connection Total Count", "mo_mgcf_succ", "mo_mgcf_total")
                .rate("VoLTE MO to ICSCF Connection Rate", "VoLTE MO to ICSCF Connection Success Count",
                        "VoLTE MO to ICSCF Connection Total Count", "mo_icscf_succ", "mo_icscf_total")
                .rate("VoLTE Voice Connection Rate", "VoLTE Voice Connection Success Count",
                        "VoLTE Voice Connection Total Count", "voice_conn_succ", "voice_conn_total")
                .rate("VoLTE Video Connection Rate", "VoLTE Video Connection Success Count",
                        "VoLTE Video Connection Total Count", "video_conn_succ", "video_conn_total")
                .rate("VoLTE Voice MO Connection Rate", "VoLTE Voice MO Connection Success Count",
                        "VoLTE Voice MO Connection Total Count", "voice_mo_succ", "voice_mo_total")
                .rate("VoLTE Video MO Connection Rate", "VoLTE Video MO Connection Success Count",
                        "VoLTE Video MO Connection Total Count", "video_mo_succ", "video_mo_total")
                .rate("VoLTE Voice MT Connection Rate", "VoLTE Voice MT Connection Success Count",
                        "VoLTE Voice MT Connection Total Count", "voice_mt_succ", "voice_mt_total")
                .rate("VoLTE Video MT Connection Rate", "VoLTE Video MT Connection Success Count",
                        "VoLTE Video MT Connection Total Count", "video_mt_succ", "video_mt_total")
                .rate("IMS MT Connection Rate", "IMS MT Connection Success Count", "IMS MT Connection Total Count",
                        "ims_mt_succ", "ims_mt_total"));
    }

    // ------------------------------------------------------------------------------------------------
    // 3) TCP summary
    // ------------------------------------------------------------------------------------------------

    public static Dataset<Row> tcpKpis(Dataset<Row> tcp) {
        Column connOk = col("TCP_Connections_Status").equalTo(0);

        Dataset<Row> agg = tcp
                .filter(hasCountry())
                .groupBy(col("RECORD_DATE"), col("Country"), col("Region"), col("Province"), col("City"),
                        tcpAccessType(col("RAT")).alias("Access_Group"))
                .agg(sumIf(connOk, "Downlink_RTT_Long_Numbers").alias("rtt_long"),
                        sumIf(connOk, "Downlink_RTT_Times").alias("rtt_total"),
                        sum("TCP_Downlink_Retransmitted_Packets_with_Payload").alias("retrans"),
                        sum("TCP_Downlink_Packets_With_Payload").alias("total_packets"),
                        sum("Downlink_Traffic_Bytes").alias("downlink_volume"),
                        sum("TCP_Connection_Success_Count").alias("tcp_conn_succ"),
                        sum("Total_TCP_Connection_Count").alias("tcp_conn_total"));

        return explodeKpis(agg, new KpiList()
                .kpi("Client Side RTT Long Rate", lit(100).multiply(col("rtt_long")), col("rtt_total"))
                .kpi("Client Side RTT Long Times", col("rtt_long"), lit(0))
                .kpi("Client Side RTT Total Count", col("rtt_total"), lit(0))

                .kpi("Downlink TCP Retransmission Rate", lit(100).multiply(col("retrans")), col("total_packets"))
                .kpi("Downlink TCP Retransmitted Packets Count", col("retrans"), lit(0))
                .kpi("Downlink TCP Total Packets Count", col("total_packets"), lit(0))

                .kpi("Downlink Effective Volume (Bytes)", col("downlink_volume"), lit(0))

                .kpi("TCP Connection Success Rate", lit(100).multiply(col("tcp_conn_succ")), col("tcp_conn_total"))
                .kpi("TCP Connection Success Count", col("tcp_conn_succ"), lit(0))
                .kpi("Total TCP Connection Count", col("tcp_conn_total"), lit(0)));
    }

    // ------------------------------------------------------------------------------------------------
    // 4) All KPIs
    // ------------------------------------------------------------------------------------------------

    /** all_kpi: the KPI rows of the three sources together. */
    public static Dataset<Row> allKpis(Dataset<Row> reg, Dataset<Row> call, Dataset<Row> tcp) {
        return registerKpis(reg).unionByName(callKpis(call)).unionByName(tcpKpis(tcp));
    }

    // ------------------------------------------------------------------------------------------------
    // 5) The final SELECT: add the values of the days before
    // ------------------------------------------------------------------------------------------------

    /**
     * The final SELECT of the query.
     *
     * @param kpi  result of allKpis() for the window to calculate
     * @param prev the rows of summary_alarm_baseline of the same window one day earlier (BASELINE_COLUMNS)
     *
     * Only ONE join (to the day before) is made. num_d2 .. num_d7 are copied from the num_d1 .. num_d6 of that row.
     * This is the same idea as in the query, so a missing day (a hole) makes the older days of that chain NULL.
     */
    public static Dataset<Row> withBaseline(Dataset<Row> kpi, Dataset<Row> prev) {
        Dataset<Row> a = kpi.alias("a");
        Dataset<Row> b = prev.alias("b");

        Column on = b.col("RECORD_DATE").equalTo(a.col("RECORD_DATE").minus(expr("INTERVAL 1 DAY")))
                .and(b.col("Country").eqNullSafe(a.col("Country")))
                .and(b.col("Region").eqNullSafe(a.col("Region")))
                .and(b.col("Province").eqNullSafe(a.col("Province")))
                .and(b.col("City").eqNullSafe(a.col("City")))
                .and(b.col("Access_Type").eqNullSafe(a.col("Access_Type")))
                .and(b.col(KPI_NAME).eqNullSafe(a.col(KPI_NAME)));

        return a.join(b, on, "left").select(
                a.col("RECORD_DATE"), a.col("Access_Type"), a.col("Country"), a.col("Region"), a.col("Province"), a.col("City"),
                a.col(KPI_NAME), a.col("kpi_num"), a.col("kpi_denom"),
                b.col("kpi_num").alias("num_d1"), b.col("kpi_denom").alias("denom_d1"),
                b.col("num_d1").alias("num_d2"), b.col("denom_d1").alias("denom_d2"),
                b.col("num_d2").alias("num_d3"), b.col("denom_d2").alias("denom_d3"),
                b.col("num_d3").alias("num_d4"), b.col("denom_d3").alias("denom_d4"),
                b.col("num_d4").alias("num_d5"), b.col("denom_d4").alias("denom_d5"),
                b.col("num_d5").alias("num_d6"), b.col("denom_d5").alias("denom_d6"),
                b.col("num_d6").alias("num_d7"), b.col("denom_d6").alias("denom_d7"));
    }

    // ------------------------------------------------------------------------------------------------
    // The key hash
    // ------------------------------------------------------------------------------------------------

    /**
     * Adds the column key_hash (a number) made from KEY_COLUMNS. The column is put right after RECORD_DATE.
     *
     * It is the same method as generateKeyHash() of the summary jobs: the values are changed to text (NULL is an empty
     * text), joined with "-", and xxhash64 makes the number. The same place, access type and KPI always give the same
     * key_hash, in every window.
     */
    public static Dataset<Row> withKeyHash(Dataset<Row> ds) {
        StringBuilder nvl = new StringBuilder("concat_ws('-',");
        for (int i = 0; i < KEY_COLUMNS.length; i++) {
            nvl.append(i == 0 ? "" : ", ").append("nvl(cast (").append(KEY_COLUMNS[i]).append(" as string),'')");
        }
        nvl.append(")");
        Dataset<Row> hashed = ds.withColumn("key_hash", xxhash64(expr(nvl.toString())));

        // the order of the columns: RECORD_DATE, key_hash, then the others (like the table)
        java.util.List<Column> columns = new java.util.ArrayList<Column>();
        columns.add(col("RECORD_DATE"));
        columns.add(col("key_hash"));
        for (String name : ds.columns()) {
            if (!name.equals("RECORD_DATE")) {
                columns.add(col(name));
            }
        }
        return hashed.select(columns.toArray(new Column[0]));
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    /** One KPI: its name, and how numerator and denominator are taken from the aggregated columns. */
    private static final class Kpi {
        final String name;
        final Column numerator;
        final Column denominator;

        Kpi(String name, Column numerator, Column denominator) {
            this.name = name;
            this.numerator = numerator;
            this.denominator = denominator;
        }
    }

    /** A list of KPIs, built step by step. */
    private static final class KpiList {
        final java.util.List<Kpi> items = new java.util.ArrayList<Kpi>();

        /** One KPI with its own numerator and denominator. */
        KpiList kpi(String name, Column numerator, Column denominator) {
            items.add(new Kpi(name, numerator, denominator));
            return this;
        }

        /**
         * The three KPIs that the SQL makes from one pair of columns (success, total):
         *   rate:          numerator = 100 * success, denominator = total
         *   success count: numerator = success,       denominator = 0
         *   total count:   numerator = total,         denominator = 0
         */
        KpiList rate(String rateName, String successName, String totalName, String success, String total) {
            kpi(rateName, lit(100).multiply(col(success)), col(total));
            kpi(successName, col(success), lit(0));
            kpi(totalName, col(total), lit(0));
            return this;
        }
    }

    /**
     * Turns the aggregated data (one row for each time / place / access type) into KPI rows:
     * one row for each KPI. Access_Group is renamed to Access_Type here.
     */
    private static Dataset<Row> explodeKpis(Dataset<Row> agg, KpiList kpis) {
        Column[] structs = new Column[kpis.items.size()];
        for (int i = 0; i < structs.length; i++) {
            Kpi k = kpis.items.get(i);
            // All values are BIGINT, as the columns of summary_alarm_baseline.
            structs[i] = struct(lit(k.name).alias("kpi_name"),
                    k.numerator.cast("long").alias("numerator"),
                    k.denominator.cast("long").alias("denominator"));
        }

        return agg.select(
                        col("RECORD_DATE"), col("Access_Group").alias("Access_Type"),
                        col("Country"), col("Region"), col("Province"), col("City"),
                        explode(array(structs)).alias("kpi"))
                .select(col("RECORD_DATE"), col("Access_Type"), col("Country"), col("Region"), col("Province"), col("City"),
                        col("kpi.kpi_name").alias(KPI_NAME),
                        col("kpi.numerator").alias("kpi_num"),
                        col("kpi.denominator").alias("kpi_denom"));
    }
}