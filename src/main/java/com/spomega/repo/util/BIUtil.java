package com.spomega.repo.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 
 * Utility file for BI
 */
public class BIUtil {

    protected final static String OK = "Ok";
    protected final static String FAILED = "Failed";
    protected final static int START_YEAR = 2016;
    protected final static Long DEFAULT_TIME = 1457518321908L;

    // <editor-fold defaultstate="collapsed" desc="Table Names">
    protected final static String TABLE_AGENT  = "bi_agent";
    protected final static String TABLE_COMMUNITY  = "bi_community";
    protected final static String TABLE_FARMER = "bi_farmer";
    protected final static String TABLE_FARM = "bi_farm";
    protected final static String TABLE_INFO = "bi_info";
    protected final static ArrayList<String> TABLES = new ArrayList<>(Arrays.asList(TABLE_AGENT , TABLE_COMMUNITY , TABLE_FARMER, TABLE_FARM, TABLE_INFO));
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Data Set Names">
    protected final static String DATA_SET_AGENT = "agent_data";
    protected final static String DATA_SET_COMMUNITY = "community_data";
    protected final static String DATA_SET_FARMER = "farmer_data";
    protected final static String DATA_SET_FARM = "farm_data";

    protected String getIndicatorTable(String data_set) throws Exception {
        switch (data_set) {
            case DATA_SET_AGENT: return TABLE_AGENT;
            case DATA_SET_COMMUNITY: return TABLE_COMMUNITY;
            case DATA_SET_FARMER: return TABLE_FARMER;
            case DATA_SET_FARM: return TABLE_FARM;
            default:
                throw new Exception("Invalid data set");
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Partner Names">
    protected final static String PARTNER_ACDI = "ACDIVOCA";
    protected final static String PARTNER_MOFA = "MOFA";
    protected final static String PARTNER_GF = "GRAMEEN";
    protected final static String PARTNER_CIF = "CIFCSF";
    protected final static String PARTNER_AIS_BA = "AIS_BA";
    protected final static String PARTNER_AIS_UE = "AIS_UE";
    protected final static String PARTNER_AIS_UW = "AIS_UW";
    protected final static String PARTNER_AIS_NR = "AIS_NR";
    protected final static String PARTNER_AIS_VR = "AIS_VR";
    protected final static String[] PARTNERS = {PARTNER_ACDI, PARTNER_MOFA, PARTNER_CIF, PARTNER_GF,
                                                PARTNER_AIS_BA, PARTNER_AIS_UE, PARTNER_AIS_UW,
                                                PARTNER_AIS_NR, PARTNER_AIS_VR};
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Date/time Helpers">
    protected static int getCurrentYear() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.YEAR);
    }

    public List<String> getYears() {
        List<String> years = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        for(int i=year; i >= START_YEAR; i--) {
            years.add(String.valueOf(i));
        }
        return years;
    }

    protected static int getNumOfAxisMonths() {
        int year = getCurrentYear();
        int start_year = (year-1 < START_YEAR) ? START_YEAR : (year-1);
        return ((year-start_year)+1) * 12;
    }

    protected static String[] getYearMonthAxisValues() {
        int year = getCurrentYear();
        int start_year = (year-1 < START_YEAR) ? START_YEAR : (year-1);
        String[] months = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
        String[] values = new String[getNumOfAxisMonths()];

        int c=0;
        for(int y=start_year; y<=year; y++) {
            for(String m : months) { values[c] = m + " " + y; c++; }
        }
        return values;
    }

    protected static int getYearMonthIdx(int year, int month) {
        return ((year-START_YEAR) * 12) + (month-1);
    }

    protected static ArrayList<Integer> getDateFromCode(String code) {
        ArrayList<Integer> dates = new ArrayList<>();

        try {
            DateFormat format = new SimpleDateFormat("MMM dd HH:mm:ss z yyyy");
            Date date = format.parse(code.substring(3).trim());
            dates = getDateFromTime(date.getTime());
            return dates;
        } catch (Exception e) {
            System.out.println(e);
        }
        return dates;
    }

    protected static ArrayList<Integer> getDateFromTime(Long time) {
        ArrayList<Integer> dates = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        Long t = (time==null) ? DEFAULT_TIME : time;
        cal.setTimeInMillis(t);
        dates.add(cal.get(Calendar.YEAR));
        dates.add(cal.get(Calendar.MONTH));
        return dates;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="MySQL DB/Table Management">

    public static boolean databaseExist() throws Exception {
        return DBUtil.getInstance().checkDBExists(null);
    }

    public static void createDatabase() throws Exception {
        DBUtil.getInstance().createDatabase(null);
    }

    public static boolean tablesExist() throws Exception {
        boolean exists = false;

        try {
            int tblCount = 0;
            ResultSet rs = DBUtil.getInstance().getMysqlConnection().getMetaData().getTables(null, null, "%", null);
            while (rs.next()) {
                if (TABLES.contains(rs.getString(3))) { tblCount++; }
            }
            exists = (tblCount==TABLES.size());
        } catch (Exception e) {
            throw(e);
        }

        return exists;
    }

    public static void createTables(Boolean fillTables) throws Exception {
        try {
            String time = "year INTEGER, month INTEGER";
            String common = "actual INTEGER, " + time;
            String partner = ", partner VARCHAR(50)";
            String community = ", community VARCHAR(50)";

            String farmMeta = ", acres_planned INTEGER, acres_actual INTEGER, yield_planned INTEGER, yield_actual INTEGER"
                    + ", price_planned INTEGER, price_actual INTEGER, crop VARCHAR(50), season VARCHAR(20)"
                    + ", partner VARCHAR(50), `agent_id` VARCHAR(100) NOT NULL, farmer_id VARCHAR(50)"
                    + ", `gender` VARCHAR(10) NULL , `age` INT NULL , `latitude` VARCHAR(50) NULL "
                    + ", `longitude` VARCHAR(50) NULL , `location` VARCHAR(100) NULL , `pp` INT NOT NULL DEFAULT '0' DEFAULT '0' , `fmp` INT NOT NULL DEFAULT '0' DEFAULT '0' , `fmp_update` INT NOT NULL DEFAULT '0' DEFAULT '0' , `measured` INT NOT NULL DEFAULT '0' DEFAULT '0' , `assessed` INT NOT NULL DEFAULT '0' DEFAULT '0'"
                    + ", `credit_planned` INT NOT NULL DEFAULT '0' , `credit_actual` INT NOT NULL DEFAULT '0', `credit_type` VARCHAR(255) NOT NULL , `ob` INT NOT NULL DEFAULT '0' , `cash_planned` INT NOT NULL DEFAULT '0' , `cash_actual` INT NOT NULL DEFAULT '0' , `cash_amount_planned` FLOAT NOT NULL , `cash_amount_actual` FLOAT NOT NULL , `cash_payback_planned` FLOAT NOT NULL , `cash_payback_actual` FLOAT NOT NULL , `input_planned` INT NOT NULL DEFAULT '0' , `input_actual` INT NOT NULL DEFAULT '0' , `input_payback_planned` FLOAT NOT NULL , `input_payback_actual` FLOAT NOT NULL , `seed_planned` INT NOT NULL DEFAULT '0' , `seed_actual` INT NOT NULL DEFAULT '0' , `fertilizer_planned` INT NOT NULL DEFAULT '0' , `fertilizer_actual` INT NOT NULL DEFAULT '0' , `preplanh_planned` INT NOT NULL DEFAULT '0' , `preplanh_actual` INT NOT NULL DEFAULT '0' , `postplanh_planned` INT NOT NULL DEFAULT '0' , `postplanh_actual` INT NOT NULL DEFAULT '0' , `plough_planned` INT NOT NULL DEFAULT '0' , `plough_actual` INT NOT NULL DEFAULT '0' , `handling_planned` INT NOT NULL DEFAULT '0' , `handling_actual` INT NOT NULL DEFAULT '0' , `transport_planned` INT NOT NULL DEFAULT '0' , `transport_actual` INT NOT NULL DEFAULT '0' , `storage_planned` INT NOT NULL DEFAULT '0' , `storage_actual` INT NOT NULL DEFAULT '0'"
                    + ", `trial` INT NOT NULL DEFAULT '0', `rc` INT NOT NULL DEFAULT '0', `preph` INT NOT NULL DEFAULT '0', `postph` INT NOT NULL DEFAULT '0', `inorg` INT NOT NULL DEFAULT '0' , `thresh` INT NOT NULL DEFAULT '0'";

            String info = " `property` varchar(255) NOT NULL, `description` text NOT NULL, `value` varchar(255) NOT NULL, `modified_by` int(11) NOT NULL, `created_at` timestamp NOT NULL, `updated_at` timestamp NOT NULL";

            DBUtil.getInstance().createTable(TABLE_AGENT, common + partner);
            DBUtil.getInstance().createTable(TABLE_COMMUNITY, common);
            DBUtil.getInstance().createTable(TABLE_FARMER, common + partner + community);
            DBUtil.getInstance().createTable(TABLE_FARM, time + farmMeta);
            DBUtil.getInstance().createTable(TABLE_INFO, info);


            String sql = "INSERT INTO "+TABLE_INFO+" (`id`, `property`, `description`, `value`, `modified_by`, `created_at`, `updated_at`) VALUES "
            + "(1, 'MOFA_FARMER_TARGET', 'Number of farmers MOFA agents need to register by end of project.', '1000', 1, '2016-08-10 12:15:19', '2016-08-02 11:50:22'), "
            + "(2, 'ACDIVOCA_FARMER_TARGET', 'Number of farmers ACDI VOCA farmers need to register by end of the project.', '4000', 1, '2016-08-10 12:15:37', '2016-08-02 11:50:22'), "
            + "(5, 'GRAMEEN_FARMER_TARGET', 'Number of farmers Grameen needs to register before the end of the project', '5000', 1, '2016-08-02 12:37:44', '2016-08-02 11:53:17'), "
            + "(6, 'CIFCSF_FARMER_TARGET', 'Number of farmers CIFCSF needs to register before the end of the project.', '5000', 1, '2016-08-02 11:55:50', '2016-08-02 11:55:50'), "
            + "(7, 'AIS_BA_FARMER_TARGET', 'Farmer target for AIS-BA', '2500', 1, '2016-08-10 12:14:51', '2016-08-10 12:14:51'), "
            + "(8, 'AIS_UE_FARMER_TARGET', 'Farmer target for AIS-UE', '3000', 1, '2016-08-10 12:18:43', '2016-08-10 12:16:29'), "
            + "(9, 'AIS_UW_FARMER_TARGET', 'Farmer target for AIS-UW', '3000', 1, '2016-08-10 12:18:31', '2016-08-10 12:18:31'), "
            + "(10, 'AIS_NR_FARMER_TARGET', 'Farmer target for AIS-NR', '6000', 1, '2016-08-10 12:18:31', '2016-08-10 12:18:31'), "
            + "(11, 'AIS_VR_FARMER_TARGET', 'Farmer target for AIS-VR', '2500', 1, '2016-08-10 12:18:31', '2016-08-10 12:18:31'), "
            + "(12, 'ACDIVOCA_AGENT_TARGET', 'Number of agents to register', '40', 1, '2016-08-10 13:37:52', '2016-08-10 13:30:03'), "
            + "(13, 'MOFA_AGENT_TARGET', 'Number of agents to register', '10', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(14, 'CIFCSF_AGENT_TARGET', 'Number of agents to register', '0', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(15, 'GRAMEEN_AGENT_TARGET', 'Number of agents to register', '1', 1, '2016-08-10 13:34:31', '2016-08-10 13:33:58'), "
            + "(16, 'AIS_BA_AGENT_TARGET', 'Number of agents to register', '25', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(17, 'AIS_UE_AGENT_TARGET', 'Number of agents to register', '30', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(18, 'AIS_UW_AGENT_TARGET', 'Number of agents to register', '30', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(19, 'AIS_NR_AGENT_TARGET', 'Number of agents to register', '60', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58'), "
            + "(20, 'AIS_VR_AGENT_TARGET', 'Number of agents to register', '25', 1, '2016-08-10 13:33:58', '2016-08-10 13:33:58')";

           DBUtil.getInstance().runSQLUpdate(sql);

            if (fillTables) { BIDataManager.getInstance().update(); }
        } catch (SQLException e) {
            throw(e);
        } catch (Exception e) {
            throw(e);
        }
    }

    // </editor-fold>
}
