package com.spomega.repo.util;

import org.apache.commons.lang.WordUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by David on 8/19/2016.
 * Main interface file to Neo4j and MySQL data stores
 */
public class BIDataManager extends BIUtil {

    private final static String TAG = BIDataManager.class.getName();

    private String data_set;

    public static BIDataManager instance;

    // <editor-fold defaultstate="collapsed" desc="Constructors">
    public static BIDataManager getInstance() {
        instance = new BIDataManager(null);
        return instance;
    }

    public static BIDataManager getInstance(String data_set) {
        instance = new BIDataManager(data_set);
        return instance;
    }

    protected BIDataManager(String i) {
        data_set = (i==null) ? "all" : i;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Get data methods">
    public String getTotalData() throws Exception {
        String total = "0";

        try {
            String sql = "SELECT SUM(actual) AS total FROM " + getIndicatorTable(data_set);
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            total =  rs.getString(rs.findColumn("total"));
            System.out.println("Total " + total);
        } catch (Exception e) {
            throw(e);
        }

        return total;
    }

    public JSONObject getPartnerRegistrationData(String partner) throws Exception {
        JSONObject x = new JSONObject();
        x.put("target", 0);
        x.put("total", 0);
        x.put("progress", 0);

        try {
            String targetCol = partner + ((data_set.equals(DATA_SET_AGENT)) ? "_AGENT_TARGET" : "_FARMER_TARGET");
            String sql =  "SELECT IFNULL(SUM(actual), 0) as total "
                    + "       , IFNULL(i.value,0) as target"
                    + "       , CEILING(IF(i.value is null OR SUM(actual) is null,0,(SUM(actual) / i.value)*100)) as progress"
                    + "  FROM " + getIndicatorTable(data_set)
                    + " RIGHT JOIN bi_info i ON i.property = '"+ targetCol + "'"
                    + " WHERE partner = '"+ partner + "'";
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            x.put("target", rs.getString(rs.findColumn("target")));
            x.put("total", rs.getString(rs.findColumn("total")));
            x.put("progress", rs.getString(rs.findColumn("progress")));

        } catch(Exception e) {
            throw(e);
        }

        return x;
    }

    public JSONObject getPartnerFMPData(String partner) throws Exception {
        try {
            return getPartnerData(partner, "fmp");
        } catch(Exception e) {
            throw(e);
        }
    }

    public JSONObject getPartnerPPData(String partner) throws Exception {
        try {
            return getPartnerData(partner, "pp");
        } catch(Exception e) {
            throw(e);
        }
    }

    public JSONObject getPartnerFMPUpdateData(String partner) throws Exception {
        try {
            return getPartnerData(partner, "fmp_update");
        } catch(Exception e) {
            throw(e);
        }
    }

    public JSONObject getPartnerFarmsData(String partner) throws Exception {
        try {
            JSONObject m = getPartnerData(partner, "measured");
            JSONObject y = getPartnerData(partner, "assessed");
            JSONObject x = new JSONObject();
            x.put("measured_total", m.get("total"));
            x.put("measured_progress", m.get("progress"));
            x.put("assessed_total", y.get("total"));
            x.put("assessed_progress", y.get("progress"));
            return x;
        } catch(Exception e) {
            throw(e);
        }
    }

    public JSONObject getEChartLineData() throws Exception {
        JSONObject x = new JSONObject();
        x.put("legend", "[]");
        x.put("xAxis", "[]");
        x.put("series", "{}");

        HashMap<String, String[]> series_data = new HashMap<>();
        String series_tpl = "{name: 'PARTNER', type: 'line', smooth: true, itemStyle: { normal: { areaStyle: { type: 'default' } } }, data: [DATA] },";

        try {
            for(String p : PARTNERS) { series_data.put(p, new String[getNumOfAxisMonths()]); }

            String sql = "SELECT d.partner as p, d.y as `year` "
                    + ", SUM(IF(d.m=1,s,0)) as Jan"
                    + ", SUM(IF(d.m=2,s,0)) as Feb"
                    + ", SUM(IF(d.m=3,s,0)) as Mar"
                    + ", SUM(IF(d.m=4,s,0)) as Apr"
                    + ", SUM(IF(d.m=5,s,0)) as May"
                    + ", SUM(IF(d.m=6,s,0)) as Jun"
                    + ", SUM(IF(d.m=7,s,0)) as Jul"
                    + ", SUM(IF(d.m=8,s,0)) as Aug"
                    + ", SUM(IF(d.m=9,s,0)) as Sep"
                    + ", SUM(IF(d.m=10,s,0)) as Oct"
                    + ", SUM(IF(d.m=11,s,0)) as Nov"
                    + ", SUM(IF(d.m=12,s,0)) as `Dec`"
                    + " FROM (SELECT partner, `year` y, `month` m, sum(actual) s"
                    + "         FROM " + getIndicatorTable(data_set)
                    + "         WHERE `year` >= YEAR(NOW()) - 1"
                    + "         GROUP BY partner, `year`, `month`) d"
                    + " GROUP BY d.partner, d.y"
                    + " ORDER BY d.partner, d.y";
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);

            while (rs.next()) {
                String partner = rs.getString(rs.findColumn("p"));
                int year = rs.getInt(rs.findColumn("year"));
                String[] d = series_data.get(partner);
                d[getYearMonthIdx(year, 1)] = rs.getString(rs.findColumn("Jan"));
                d[getYearMonthIdx(year, 2)] = rs.getString(rs.findColumn("Feb"));
                d[getYearMonthIdx(year, 4)] = rs.getString(rs.findColumn("Mar"));
                d[getYearMonthIdx(year, 5)] = rs.getString(rs.findColumn("Apr"));
                d[getYearMonthIdx(year, 5)] = rs.getString(rs.findColumn("May"));
                d[getYearMonthIdx(year, 6)] = rs.getString(rs.findColumn("Jun"));
                d[getYearMonthIdx(year, 7)] = rs.getString(rs.findColumn("Jul"));
                d[getYearMonthIdx(year, 8)] = rs.getString(rs.findColumn("Aug"));
                d[getYearMonthIdx(year, 9)] = rs.getString(rs.findColumn("Sep"));
                d[getYearMonthIdx(year, 10)] = rs.getString(rs.findColumn("Oct"));
                d[getYearMonthIdx(year, 11)] = rs.getString(rs.findColumn("Nov"));
                d[getYearMonthIdx(year, 12)] = rs.getString(rs.findColumn("Dec"));
                series_data.put(partner, d);
            }

            String series = "";
            Iterator it = series_data.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                series += series_tpl.replace("PARTNER", ((String)pair.getKey())).replace("DATA", String.join(",", ((String[])pair.getValue())));
                it.remove();
            }
            x.put("legend", "['" + String.join("', '", PARTNERS) + "']");
            x.put("xAxis", "['"+String.join("','", getYearMonthAxisValues())+"']");
            x.put("series", "[" + series + "]");
        } catch(Exception e) {
            throw(e);
        }

        return x;
    }

    public JSONObject getEChartLineDataByCommunityForPartner(String partner) throws Exception {
        JSONObject x = new JSONObject();
        x.put("legend", "[]");
        x.put("xAxis", "[]");
        x.put("series", "{}");

        HashMap<String, List<String>> series_data = new HashMap<>();
        String series_tpl = "{name: 'YEAR', type: 'bar', data: [DATA] },";

        try {
            List<String> years = getYears();
            List<String> communities = new ArrayList<>();

            for(String y : years) { series_data.put(y, new ArrayList()); }

            String sql = "SELECT community, `year` y, sum(actual) s"
                    + "     FROM " + getIndicatorTable(data_set)
                    + "    WHERE partner = '"+partner+"'"
                    + "    GROUP BY community, `year`"
                    + "    ORDER BY community, `year`";
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);

            while (rs.next()) {
                int sum = rs.getInt(rs.findColumn("s"));
                String year = String.valueOf(rs.getInt(rs.findColumn("y")));
                String c = WordUtils.capitalizeFully(rs.getString(rs.findColumn("community")));
                if (!communities.contains(c)) { communities.add(c); }
                List<String> d = series_data.get(year);
                d.add(String.valueOf(sum));
                series_data.put(year, d);
            }

            String series = "";
            Iterator it = series_data.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String year = (String) pair.getKey();
                List<String> d = (List<String>) pair.getValue();
                series += series_tpl.replace("YEAR", year).replace("DATA", String.join(",", d));
                it.remove();
            }

            x.put("legend", "['" + String.join("','",years)+"']");
            x.put("xAxis", "['"+String.join("','", communities) +"']");
            x.put("series", "[" + series + "]");
        } catch(Exception e) {
            throw(e);
        }

        return x;
    }

    public String getSparkLineData() throws Exception {
        String data = "0,0,0,0,0,0,0,0,0,0,0,0";

        try {
            String sql = "SELECT d.y as `year` "
                    + ", SUM(IF(d.m=1,s,0)) as Jan"
                    + ", SUM(IF(d.m=2,s,0)) as Feb"
                    + ", SUM(IF(d.m=3,s,0)) as Mar"
                    + ", SUM(IF(d.m=4,s,0)) as Apr"
                    + ", SUM(IF(d.m=5,s,0)) as May"
                    + ", SUM(IF(d.m=6,s,0)) as Jun"
                    + ", SUM(IF(d.m=7,s,0)) as Jul"
                    + ", SUM(IF(d.m=8,s,0)) as Aug"
                    + ", SUM(IF(d.m=9,s,0)) as Sep"
                    + ", SUM(IF(d.m=10,s,0)) as Oct"
                    + ", SUM(IF(d.m=11,s,0)) as Nov"
                    + ", SUM(IF(d.m=12,s,0)) as `Dec`"
                    + " FROM (SELECT `year` y, `month` m, sum(actual) s"
                    + "         FROM " + getIndicatorTable(data_set)
                    + "         WHERE `year` >= YEAR(NOW()) - 1"
                    + "         GROUP BY `year`, `month`) d"
                    + " GROUP BY d.y"
                    + " ORDER BY d.y";
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);

            int year = getCurrentYear();
            while (rs.next()) {
                if (year == rs.getInt(rs.findColumn("year"))) {
                    data = rs.getString(rs.findColumn("Jan")) + ",";
                    data += rs.getString(rs.findColumn("Feb")) + ",";
                    data += rs.getString(rs.findColumn("Mar")) + ",";
                    data += rs.getString(rs.findColumn("Apr")) + ",";
                    data += rs.getString(rs.findColumn("May")) + ",";
                    data += rs.getString(rs.findColumn("Jun")) + ",";
                    data += rs.getString(rs.findColumn("Jul")) + ",";
                    data += rs.getString(rs.findColumn("Aug")) + ",";
                    data += rs.getString(rs.findColumn("Sep")) + ",";
                    data += rs.getString(rs.findColumn("Oct")) + ",";
                    data += rs.getString(rs.findColumn("Nov")) + ",";
                    data += rs.getString(rs.findColumn("Dec"));
                    return data;
                }
            }
        } catch (Exception e) {
            throw(e);
        }

        return data;
    }

    public JSONObject getFarmAreaInfo(String partner, String season, String year) throws Exception {
        JSONObject x = new JSONObject();
        x.put("total_acres_planned", 0); x.put("total_acres_actual", 0);
        x.put("largest_acre_planned", 0); x.put("largest_acre_actual", 0);
        x.put("smallest_acre_planned", 0); x.put("smallest_acre_actual", 0);
        x.put("average_acre_planned", 0); x.put("average_acre_actual", 0);
        x.put("average_yield_planned", 0); x.put("average_yield_actual", 0);
        x.put("total_production_planned", 0); x.put("total_production_actual", 0);
        x.put("total_value_planned", 0); x.put("total_value_actual", 0);

        try {

            String where = "partner = '" + partner + "' ";
            where += (season.equalsIgnoreCase("all")  || season.equalsIgnoreCase("")) ? "" : " AND season='"+season+"'";
            where += (year.equalsIgnoreCase("all")  || year.equalsIgnoreCase("")) ? "" : " AND year="+year;

            String sql = "SELECT IFNULL(SUM(acres_planned),0) as tap, IFNULL(SUM(acres_actual),0) as taa"
                    + "     , IFNULL(MAX(acres_planned),0) as lap, IFNULL(MAX(acres_actual),0) as laa"
                    + "     , IFNULL(MIN(acres_planned),0) as sap, IFNULL(MIN(acres_actual),0) as saa"
                    + "     , IFNULL(AVG(acres_planned),0) as aap, IFNULL(AVG(acres_actual),0) as aaa"
                    + "     , IFNULL(AVG(yield_planned),0) as ayp, IFNULL(AVG(yield_actual),0) as aya"
                    + "     , IFNULL(SUM(acres_planned * yield_planned),0) as tpp, IFNULL(SUM(acres_actual * yield_actual),0) as tpa"
                    + "     , IFNULL((price_planned * SUM(acres_planned * yield_planned)),0) as vtp"
                    + "     , IFNULL((price_actual * SUM(acres_actual * yield_actual)),0) as vta"
                    + " FROM " + TABLE_FARM + " WHERE " + where;

            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            x.put("total_acres_planned", rs.getString(rs.findColumn("tap"))); x.put("total_acres_actual", rs.getString(rs.findColumn("taa")));
            x.put("largest_acre_planned", rs.getString(rs.findColumn("lap"))); x.put("largest_acre_actual", rs.getString(rs.findColumn("laa")));
            x.put("smallest_acre_planned", rs.getString(rs.findColumn("sap"))); x.put("smallest_acre_actual", rs.getString(rs.findColumn("saa")));
            x.put("average_acre_planned", rs.getString(rs.findColumn("aap"))); x.put("average_acre_actual", rs.getString(rs.findColumn("aaa")));
            x.put("average_yield_planned", rs.getString(rs.findColumn("ayp"))); x.put("average_yield_actual", rs.getString(rs.findColumn("aya")));
            x.put("total_production_planned", rs.getString(rs.findColumn("tpp"))); x.put("total_production_actual", rs.getString(rs.findColumn("tpa")));
            x.put("total_value_planned", rs.getString(rs.findColumn("vtp"))); x.put("total_value_actual", rs.getString(rs.findColumn("vta")));

        } catch(Exception e) {
            throw(e);
        }

        return x;
    }

    public JSONObject getFarmCreditInfo(String partner, String season, String year, boolean onlyOB) throws Exception {

        JSONObject x = new JSONObject();
        x.put("credit_planned", 0); x.put("credit_actual", 0);
        x.put("cash_planned", 0); x.put("cash_actual", 0);
        x.put("input_planned", 0); x.put("input_actual", 0);
        x.put("cash_amount_planned", 0); x.put("cash_amount_actual", 0);
        x.put("cash_payback_planned", 0); x.put("cash_payback_actual", 0);
        x.put("input_payback_planned", 0); x.put("input_payback_actual", 0);
        x.put("seed_planned", 0); x.put("seed_actual", 0);
        x.put("fertilizer_planned", 0); x.put("fertilizer_actual", 0);
        x.put("preplanh_planned", 0); x.put("preplanh_actual", 0);
        x.put("postplanh_planned", 0); x.put("postplanh_actual", 0);
        x.put("plough_planned", 0); x.put("plough_actual", 0);
        x.put("handling_planned", 0); x.put("handling_actual", 0);
        x.put("transport_planned", 0); x.put("transport_actual", 0);
        x.put("storage_planned", 0); x.put("storage_actual",0);

        try {
            String where = "partner = '" + partner + "' ";
            where += (season.equalsIgnoreCase("all")  || season.equalsIgnoreCase("")) ? "" : " AND season='"+season+"'";
            where += (year.equalsIgnoreCase("all")  || year.equalsIgnoreCase("")) ? "" : " AND year="+year;
            where += (onlyOB) ? " AND ob=1" : "";

            String sql = "SELECT IFNULL(SUM(credit_planned),0) as crp, IFNULL(SUM(credit_actual),0) as cra, "
                    +"           IFNULL(SUM(cash_planned),0) as csp, IFNULL(SUM(cash_actual),0) as csa,"
                    +"           IFNULL(SUM(input_planned),0) as ip, IFNULL(SUM(input_actual),0) as ia,"
                    +"           IFNULL(SUM(cash_amount_planned),0) as cap, IFNULL(SUM(cash_amount_actual),0) as caa,"
                    +"           IFNULL(SUM(cash_payback_planned),0) as cpp, IFNULL(SUM(cash_payback_actual),0) as cpa,"
                    +"           IFNULL(SUM(input_payback_planned),0) as ipp, IFNULL(SUM(input_payback_actual),0) as ipa,"
                    +"           IFNULL(SUM(seed_planned),0) as sp, IFNULL(SUM(seed_actual),0) as sa,"
                    +"           IFNULL(SUM(fertilizer_planned),0) as fp, IFNULL(SUM(fertilizer_actual),0) as fa,"
                    +"           IFNULL(SUM(preplanh_planned),0) as prehp, IFNULL(SUM(preplanh_actual),0) as preha,"
                    +"           IFNULL(SUM(postplanh_planned),0) as posthp, IFNULL(SUM(postplanh_actual),0) as postha,"
                    +"           IFNULL(SUM(plough_planned),0) as plp, IFNULL(SUM(plough_actual),0) as pla,"
                    +"           IFNULL(SUM(handling_planned),0) as hp, IFNULL(SUM(handling_actual),0) as ha,"
                    +"           IFNULL(SUM(transport_planned),0) as tp, IFNULL(SUM(transport_actual),0) as ta,"
                    +"           IFNULL(SUM(storage_planned),0) as stop, IFNULL(SUM(storage_actual),0) as stoa"
                    +"      FROM " + TABLE_FARM + " WHERE " + where;

            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            x.put("credit_planned", rs.getString(rs.findColumn("crp"))); x.put("credit_actual", rs.getString(rs.findColumn("cra")));
            x.put("cash_planned", rs.getString(rs.findColumn("csp"))); x.put("cash_actual", rs.getString(rs.findColumn("csa")));
            x.put("input_planned", rs.getString(rs.findColumn("ip"))); x.put("input_actual", rs.getString(rs.findColumn("ia")));
            x.put("cash_amount_planned", rs.getString(rs.findColumn("cap"))); x.put("cash_amount_actual", rs.getString(rs.findColumn("caa")));
            x.put("cash_payback_planned", rs.getString(rs.findColumn("cpp"))); x.put("cash_payback_actual", rs.getString(rs.findColumn("cpa")));
            x.put("input_payback_planned", rs.getString(rs.findColumn("ipp"))); x.put("input_payback_actual", rs.getString(rs.findColumn("ipa")));
            x.put("seed_planned", rs.getString(rs.findColumn("sp"))); x.put("seed_actual", rs.getString(rs.findColumn("sa")));
            x.put("fertilizer_planned", rs.getString(rs.findColumn("fp"))); x.put("fertilizer_actual", rs.getString(rs.findColumn("fa")));
            x.put("preplanh_planned", rs.getString(rs.findColumn("prehp"))); x.put("preplanh_actual", rs.getString(rs.findColumn("preha")));
            x.put("postplanh_planned", rs.getString(rs.findColumn("posthp"))); x.put("postplanh_actual", rs.getString(rs.findColumn("postha")));
            x.put("plough_planned", rs.getString(rs.findColumn("plp"))); x.put("plough_actual", rs.getString(rs.findColumn("pla")));
            x.put("handling_planned", rs.getString(rs.findColumn("hp"))); x.put("handling_actual", rs.getString(rs.findColumn("ha")));
            x.put("transport_planned", rs.getString(rs.findColumn("tp"))); x.put("transport_actual", rs.getString(rs.findColumn("ta")));
            x.put("storage_planned", rs.getString(rs.findColumn("stop"))); x.put("storage_actual",rs.getString(rs.findColumn("stoa")));

        } catch(Exception e) {
            throw(e);
        }

        return x;
    }

    public JSONObject getBehaviourChangeInfo(String crop, String gender, String location, String partner) throws Exception {
        JSONObject x = new JSONObject();
        x.put("ipt", 0); x.put("ipt_area", 0);
        x.put("is", 0); x.put("is_area", 0);
        x.put("cda", 0); x.put("cda_area", 0);
        x.put("if", 0); x.put("if_area", 0);
        x.put("preh", 0); x.put("preh_area", 0);
        x.put("posth", 0); x.put("posth_area", 0);
        x.put("pht", 0); x.put("pht_area", 0);

        try {
            String where = "1=1";
            where += (crop.equalsIgnoreCase("all")  || crop.equalsIgnoreCase("")) ? "" : " AND crop='"+crop+"'";
            where += (gender.equalsIgnoreCase("all")  || gender.equalsIgnoreCase("")) ? "" : " AND gender='"+gender+"'";
            where += (location.equalsIgnoreCase("all")  || location.equalsIgnoreCase("")) ? "" : " AND location='"+location+"'";
            where += (partner.equalsIgnoreCase("all")  || partner.equalsIgnoreCase("")) ? "" : " AND partner='"+partner+"'";

            String sql = "SELECT IFNULL(SUM(COALESCE(IF(trial>0 OR rc>0 OR inorg>0 or preph>0 or postph>0 or thresh>0,1,0))),0) as ipt " +
                         "      , IFNULL(SUM(COALESCE(IF(trial>0 OR rc>0 OR inorg>0 or preph>0 or postph>0,acres_actual,0))),0) as ipt_area " +
                         "      , IFNULL(SUM(COALESCE(IF(trial>0,1,0))),0) as `is` " +
                         "      , IFNULL(SUM(COALESCE(IF(trial>0,acres_actual,0))),0) as is_area " +
                         "      , IFNULL(SUM(COALESCE(IF(rc>0,1,0))),0) as `cda` " +
                         "      , IFNULL(SUM(COALESCE(IF(rc>0,acres_actual,0))),0) as cda_area " +
                         "      , IFNULL(SUM(COALESCE(IF(inorg>0,1,0))),0) as `if` " +
                         "      , IFNULL(SUM(COALESCE(IF(inorg>0,acres_actual,0))),0) as if_area " +
                         "      , IFNULL(SUM(COALESCE(IF(preph>0,1,0))),0) as `preh` " +
                         "      , IFNULL(SUM(COALESCE(IF(preph>0,acres_actual,0))),0) as preh_area " +
                         "      , IFNULL(SUM(COALESCE(IF(postph>0,1,0))),0) as `posth` " +
                         "      , IFNULL(SUM(COALESCE(IF(postph>0,acres_actual,0))),0) as posth_area " +
                         "      , IFNULL(SUM(COALESCE(IF(thresh>0,1,0))),0) as `pht` " +
                         "      , IFNULL(SUM(COALESCE(IF(thresh>0,yield_actual,0))),0) as pht_area " +
                         " FROM " + TABLE_FARM + " WHERE " + where;
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            x.put("ipt", rs.getString(rs.findColumn("ipt"))); x.put("ipt_area", rs.getString(rs.findColumn("ipt_area")));
            x.put("is", rs.getString(rs.findColumn("is"))); x.put("is_area", rs.getString(rs.findColumn("is_area")));
            x.put("cda", rs.getString(rs.findColumn("cda"))); x.put("cda_area", rs.getString(rs.findColumn("cda_area")));
            x.put("if", rs.getString(rs.findColumn("if"))); x.put("if_area", rs.getString(rs.findColumn("if_area")));
            x.put("preh", rs.getString(rs.findColumn("preh"))); x.put("preh_area", rs.getString(rs.findColumn("preh_area")));
            x.put("posth", rs.getString(rs.findColumn("posth"))); x.put("posth_area", rs.getString(rs.findColumn("posth_area")));
            x.put("pht", rs.getString(rs.findColumn("pht"))); x.put("pht_area", rs.getString(rs.findColumn("pht_area")));

        } catch(Exception e) {
            throw(e);
        }
        return x;
    }

    public JSONObject getAdvisoryInfo(String crop, String gender, String location) throws Exception {
        JSONObject x = new JSONObject();
        x.put("app", 0); x.put("appv", 0);
        x.put("aph", 0); x.put("aphv", 0);
        x.put("apost", 0); x.put("apostv", 0);
        x.put("ama", 0); x.put("amav", 0);
        x.put("aa", 0); x.put("fm", 0);
        x.put("fa", 0);

        try {
            String where = "1=1";
            where += (crop.equalsIgnoreCase("all")  || crop.equalsIgnoreCase("")) ? "" : " AND crop='"+crop+"'";
            where += (gender.equalsIgnoreCase("all")  || gender.equalsIgnoreCase("")) ? "" : " AND gender='"+gender+"'";
            where += (location.equalsIgnoreCase("all")  || location.equalsIgnoreCase("")) ? "" : " AND location='"+location+"'";

            String sql = "SELECT 0 as ipt, 0 as ipt_area"
                       + "  FROM " + TABLE_FARM + " WHERE " + where;
            //ResultSet rs = ICTCDBUtil.getInstance().runSQLSelect(sql);
            // rs.next();

        } catch(Exception e) {
            throw(e);
        }
        return x;
    }

    public List<String> getCrops() {
       // String cql = "MATCH (f:FARMER) MATCH (f)-[:HAS_PRODUCTION]-(p) RETURN DISTINCT p.crop_to_cultivate_current AS c ORDER BY c";
        String cql = "MATCH (f:FARMER)  RETURN DISTINCT f.majorcrop AS c ORDER BY c";
        List<String> crops = new ArrayList<>();

        try (Transaction trx = DBUtil.getInstance().getGraphDB().beginTx()) {
            Result result = Neo4jServices.executeCypherQuery(cql);
            while (result.hasNext()) {
                Map<String, Object> c = result.next();
                crops.add(WordUtils.capitalizeFully((String) c.get("c")));
            }
            trx.success();
        } catch (Exception e) {
            System.out.println("Error pulling crops data from Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return crops;
    }

    public List<String> getLocations() {
        String cql = "MATCH (f:FARMER) WHERE f.region is not null RETURN DISTINCT lower(f.region) AS r ORDER BY r";
        List<String> locations = new ArrayList<>();

        try (Transaction trx = DBUtil.getInstance().getGraphDB().beginTx()) {
            Result result = Neo4jServices.executeCypherQuery(cql);
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                locations.add(WordUtils.capitalizeFully((String) r.get("r")));
            }
            trx.success();
        } catch (Exception e) {
            System.out.println("Error pulling locations data from Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return locations;
    }

    private JSONObject getPartnerData(String partner, String field) throws Exception {
        JSONObject x = new JSONObject();
        x.put("total", 0);
        x.put("progress", 0);

        try {
            String targetCol = partner + ((data_set.equals(DATA_SET_AGENT)) ? "_AGENT_TARGET" : "_FARMER_TARGET");
            String sql =  (data_set.equals(DATA_SET_AGENT))
                    ?   "SELECT COUNT(t.agent_id) as total "
                    + "    , IFNULL(i.value,0) as target "
                    + "    , CEILING(IF(i.value is null or COUNT(t.agent_id)=0,0,((COUNT(t.agent_id) / i.value)*100))) as progress"
                    + " FROM (SELECT agent_id, IFNULL(SUM("+field+"), 0) as done FROM "+ TABLE_FARM +
                    "        WHERE partner = '"+partner+"' GROUP BY agent_id HAVING (done > 50 )) t "
                    + " JOIN bi_info i ON i.property = '"+ targetCol +"'"

                    : "SELECT IFNULL(SUM("+field+"), 0) as total "
                    + "       , IFNULL(i.value,0) as target"
                    + "       , CEILING(IF(i.value is null OR SUM("+field+") is null,0,(SUM("+field+") / i.value)*100)) as progress"
                    + "  FROM " + TABLE_FARM
                    + " RIGHT JOIN bi_info i ON i.property = '"+ targetCol + "'"
                    + " WHERE partner = '"+ partner + "'";
            ResultSet rs = DBUtil.getInstance().runSQLSelect(sql);
            rs.next();
            x.put("target", rs.getString(rs.findColumn("target")));
            x.put("total", rs.getString(rs.findColumn("total")));
            x.put("progress", rs.getString(rs.findColumn("progress")));

        } catch(Exception e) {
            throw(e);
        }

        return x;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Update data methods">
    public HashMap<String, String> update() {
        HashMap<String, String> response = new HashMap<>();

        try {
            if (data_set.equals("all")) {
                System.out.println("doing the update");
               
                response.put(DATA_SET_AGENT, ((updateTables(DATA_SET_AGENT) ? OK : FAILED)));
                response.put(DATA_SET_COMMUNITY, ((updateTables(DATA_SET_COMMUNITY) ? OK : FAILED)));
                response.put(DATA_SET_FARMER, ((updateTables(DATA_SET_FARMER) ? OK : FAILED)));
                
                
            } else {
                response.put(data_set, ((updateTables(data_set) ? OK : FAILED)));
            }
        } catch (Exception ex) {
            response.put(data_set, FAILED);
            Logger.getLogger(TAG).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }

        System.out.println("response ---------------------" +response.toString());
        return response;
    }

    private boolean updateTables(String data_set) {

        try {
            
           String cql = getCQL(data_set);
           
           // System.out.println("Data set " + data_set + " -------" + cql);
            if (cql.equals("")) {
                System.out.println("Error pulling data from Neo4j: invalid data set name: " + data_set);
                return false;

            } else {
                HashMap<String, SQLRowObj> farmers = new HashMap<>();
                HashMap<String, SQLRowObj> agents = new HashMap<>();
                HashMap<String, SQLRowObj> communities = new HashMap<>();
                HashMap<String, SQLRowObj> farms = new HashMap<>();
              
                try (Transaction trx = DBUtil.getInstance().getGraphDB().beginTx()) {
                    System.out.println("Before cypher Farmer Execution");
                    Result result = Neo4jServices.executeCypherQuery(cql);
                   
                    ResourceIterator ri = result.columnAs("info");
                   
                    int count = 0;
                    while (ri.hasNext()) {

                        if (data_set.equals(DATA_SET_AGENT)) {
                             System.out.println("In agent data set ");
                            SQLRowObj objAgents = getSQLRowObj(DATA_SET_AGENT, ri.next(), count);
                           
                            if (objAgents != null)
                                agents = updateHashMap(agents, objAgents.id(TABLE_AGENT), objAgents);

                        } else if (data_set.equals(DATA_SET_COMMUNITY)) {
                             System.out.println("In Community data set ");
                            SQLRowObj objComms = getSQLRowObj(DATA_SET_COMMUNITY, ri.next(), count);
                            
                            if (objComms != null)
                                communities = updateHashMap(communities, objComms.id(TABLE_COMMUNITY), objComms);

                        } else if (data_set.equals(DATA_SET_FARMER)) {
                           //if (true) {
                            System.out.println("In Farmer data set ");
                            Object row = ri.next();
                            System.out.println("getting row  data" + row.toString());
                            SQLRowObj objFarmer = getSQLRowObj(DATA_SET_FARMER, row, count);
                            if (objFarmer != null)
                                farmers = updateHashMap(farmers, objFarmer.id(TABLE_FARMER),  objFarmer);

                            System.out.println("farmer size " + farmers.size());
                            count++;
                            SQLRowObj objFarms = getSQLRowObj(DATA_SET_FARM, row, count);
                            if (objFarms != null)
                                farms.put(objFarms.id(TABLE_FARM), objFarms);
                        }
                        count++;
                    }
                    trx.success();
                } catch (Exception e) {
                    System.out.println("Error pulling data from Neo4j: " + e.getMessage() + " " + e.getLocalizedMessage());
                    e.printStackTrace();
                }

                // Insert data into MySQL
                if (data_set.equals(DATA_SET_FARMER)) {
                    System.out.println("inserting into farmer table");
                    runUpdates(TABLE_FARMER, farmers);
                    runUpdates(TABLE_FARM, farms);
                } else if (data_set.equals(DATA_SET_AGENT)) {
                    runUpdates(TABLE_AGENT, agents);
                } else if (data_set.equals(DATA_SET_COMMUNITY)) {
                    runUpdates(TABLE_COMMUNITY, communities);
                }

                return true;
            }

        } catch (SQLException e){
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    private String getCQL(String indicator) {
        String cql;

        switch(indicator) {
            case DATA_SET_AGENT: cql = "MATCH (a:AGENT) return { code: a.agentcode, partner: a.agenttype, lmd: a.lastModifieddate} as info"; break;
            case DATA_SET_COMMUNITY: cql = "MATCH (f:FARMER) WITH lower(replace(f.village,\",\",\"\")) as v, MIN(f.lastModifieddate) as d RETURN { community: v, lmd: d} as info"; break;
            case DATA_SET_FARMER:
                System.out.println("getting farmer data");
                StringBuilder sb = new StringBuilder();
                sb.append("MATCH (fff:FARMER) ");
                sb.append("OPTIONAL MATCH (fff)-[:HAS_PRODUCTION]-(pc)  ");
                sb.append("WITH fff, MAX(pc.lastModifieddate) as maxp  ");
                sb.append("OPTIONAL MATCH (fff)-[:HAS_PRODUCTION]-(p) WHERE p.lastModifieddate >= maxp  ");
                sb.append("WITH fff,p  ");
                sb.append("OPTIONAL MATCH (fff)-[:HAS_PRODUCTION_UPDATE]-(u)  ");
                sb.append("WITH fff,p,MAX(u.lastModifieddate) as maxPUpdate  ");
                sb.append("OPTIONAL MATCH (fff)-[:HAS_PRODUCTION_UPDATE]-(u) WHERE u.lastModifieddate >= maxPUpdate  ");
                sb.append("WITH fff,p,u, (SUM(CASE u.nameofvarietymz WHEN NULL THEN 0 ");
				sb.append("                                          WHEN \"local variety\" THEN 0 ");
				sb.append("                                          WHEN \"other\" THEN 0 ELSE 1 END) + ");
				sb.append("               SUM(CASE u.nameofvarietyrice WHEN NULL THEN 0 ");
				sb.append("                                            WHEN \"local variety\" THEN 0 ");
				sb.append("                                            WHEN \"other\" THEN 0 ELSE 1 END) + ");
				sb.append("               SUM(CASE u.nameofvarietysoya WHEN NULL THEN 0 ");
				sb.append("                                            WHEN \"local variety\" THEN 0 ");
				sb.append("                                            WHEN \"other\" THEN 0 ELSE 1 END) + SUM(CASE u.nameofhybridmz WHEN NULL THEN 0  WHEN \"local variety\" THEN 0 WHEN \"other\" THEN 0 ELSE 1 END)) as trial, ");
				sb.append("              (SUM(CASE u.croparrangeupdate WHEN NULL THEN 0 ");
				sb.append("                                            WHEN \"Arranged rows with specific distance between rows and also between plants\" THEN 1 ");
				sb.append("                                            ELSE 0 END)) as rc, ");
				sb.append("              (SUM(CASE u.methodoflandclearing WHEN NULL THEN 0 WHEN 0 THEN 0 ");
				sb.append("                                               WHEN \"Herbicide application\" THEN 1 ");
				sb.append("                                               WHEN \"Slashing and application of herbicide\" THEN 1 ");
				sb.append("                                               ELSE 0 END)) as preph, ");
				sb.append("              (SUM(CASE u.postplantherbicidefrequency WHEN NULL THEN 0 ");
				sb.append("                                                      WHEN 0 THEN 0 ELSE 1 END)) as postph, ");
				sb.append("              (SUM(CASE u.applicationofbasalfertilizer WHEN \"YES\" THEN 1 ");
				sb.append("                                                       WHEN NULL THEN 0 ELSE 0 END) + ");
				sb.append("               SUM(CASE u.applicationoftopdressfertilizer WHEN NULL THEN 0 ");
				sb.append("                                                          WHEN \"YES\" THEN 1 ELSE 0 END)) as inorg ");
				sb.append("MERGE (ff:FARMER {Id: fff.Id}) ON MATCH SET ff.trial = trial, ff.rc = rc, ff.pph = preph, ff.postph = postph, ff.inorg = inorg ");
				sb.append("WITH ff,p,u ");
                sb.append("OPTIONAL MATCH (ff)-[:HAS_POSTHARVEST]-(h)  ");
                sb.append("WITH ff,p,u,MAX(h.lastModifieddate) as maxPH  ");
                sb.append("OPTIONAL MATCH (ff)-[:HAS_POSTHARVEST]-(h) WHERE h.lastModifieddate >= maxPH  ");
                sb.append("WITH ff,p,u,h  ");
                sb.append("OPTIONAL MATCH (ff)-[:HAS_POSTHARVEST_UPDATE]-(z)  ");
                sb.append("WITH ff,p,u,h,MAX(z.lastModifieddate) as maxPHdate  ");
                sb.append("OPTIONAL MATCH (ff)-[:HAS_POSTHARVEST_UPDATE]-(z) WHERE z.lastModifieddate >= maxPHdate  ");
                sb.append("WITH ff,p,u,h,z  ");
				sb.append("    , (SUM(CASE z.processingcombinationupdate WHEN \"All by one machine at once\" THEN 1 ");
				sb.append("                    ELSE 0 END) + ");
				sb.append("       SUM(CASE z.threshingmethodupdate WHEN \"Answer is \\\"Motorized sheller\\\"\" THEN 1 ");
				sb.append("                            ELSE 0 END) + ");
				sb.append("       SUM(CASE z.shellingmethodupdate WHEN \"Answer is \\\"Motorized sheller\\\"\" THEN 1 ");
				sb.append("                            WHEN \"combined harvester\" THEN 1 ELSE 0 END) + ");
				sb.append("       SUM(CASE z.winnowing WHEN \"Answer is \\\"Mechanical thresher/winnower\\\"\" THEN 1 ");
				sb.append("                            WHEN \"combined harvester\" THEN 1 ELSE 0 END)) as thresh ");
				sb.append("MERGE (f:FARMER {Id: ff.Id}) ON MATCH SET f.thresh = thresh ");
				sb.append("WITH f,p,u,h,z ");
                sb.append("OPTIONAL MATCH (f)-[:HAS_BASELINE_PRODUCTION]-(pp) ");
                sb.append("OPTIONAL MATCH (f)-[:HAS_FIELD_CROP_ASSESSMENT]-(aa) ");
                sb.append("OPTIONAL MATCH (f)-[:HAS_FARMCREDIT_PLAN]-(fcp)  ");
                sb.append("OPTIONAL MATCH (f)-[:HAS_FARMCREDIT_PLAN_UPDATE]-(fcpu)  ");
                sb.append("WITH f,p,u,h,z,pp,aa,fcp,fcpu  ");
                sb.append("MATCH (a:AGENT) WHERE a.Id = f.CreatedById  ");
                sb.append("WITH f,p,u,h,z,pp,aa,fcp,fcpu,MAX(a.lastModifieddate) as maxadate ");
                sb.append("MATCH (a:AGENT) WHERE a.Id = f.CreatedById AND a.lastModifieddate >= maxadate ");
                sb.append("RETURN { farmerId: f.Id  ");
                sb.append("        , agent: a.Id   ");
                sb.append("        , partner: a.agenttype   ");
                sb.append("        , bc: collect({ trial: f.trial, rc: f.rc, preph: f.pph, postph: f.postph, inorg:f.inorg, thresh: f.thresh}) ");
                sb.append("        , demo: collect({gender: f.gender, age:f.age, location: f.region, community: lower(replace(f.village,\",\",\"\"))  })  ");
                sb.append("        , time: collect ({  lmd: f.lastModifieddate ");
                sb.append("                            , season: CASE WHEN p IS NOT NULL THEN p.reference_season_current ELSE NULL END ");
                sb.append("                            , year: CASE WHEN p IS NOT NULL THEN p.reference_year_current ELSE NULL END }) ");
                sb.append("        , crop: collect({ crop: CASE WHEN p IS NOT NULL THEN p.crop_to_cultivate_current ELSE NULL END  ");
                sb.append("                          , yield_planned: CASE WHEN p IS NOT NULL THEN p.targetyieldperacre ELSE 0 END ");
                sb.append("                          , price_planned: CASE WHEN h IS NOT NULL THEN h.priceatmostsaledate ELSE 0 END  ");
                sb.append("                          , yield_actual: CASE WHEN z IS NOT NULL THEN z.total_yield_update ELSE 0 END ");
                sb.append("                          , price_actual: CASE WHEN z IS NOT NULL THEN z.priceatmostsaledate ELSE 0 END })  ");
                sb.append("        , farminfo: collect({ acres_planned: CASE WHEN p IS NOT NULL THEN p.acresofland ELSE 0 END ");
                sb.append("                              , acres_actual: CASE WHEN u IS NOT NULL THEN u.landareacultivatedupdate ELSE 0 END ");
                sb.append("                              , lat: CASE WHEN u IS NOT NULL THEN u.gpslocationplanpdupdate__Latitude ELSE NULL END ");
                sb.append("                              , long: CASE WHEN u IS NOT NULL THEN u.gpslocationplanpdupdate__Longitude ELSE NULL END ");
                sb.append("                              , pp: CASE WHEN pp IS NOT NULL THEN 1 ELSE 0 END  ");
                sb.append("                              , fmp: CASE WHEN p IS NOT NULL THEN 1 ELSE 0 END  ");
                sb.append("                              , fmp_update: CASE WHEN u IS NOT NULL THEN 1 ELSE 0 END  ");
                sb.append("                              , measured: CASE WHEN (p IS NOT NULL AND p.acresofland IS NOT NULL) THEN 1 ELSE 0 END  ");
                sb.append("                              , assessed: CASE WHEN aa IS NOT NULL THEN 1 ELSE 0 END }) ");
                sb.append("        , farmcreditplan: collect ({ ");
                sb.append("               credit_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditactualc END ");
                sb.append("               , credit_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditactualupdateu END ");
                sb.append("               , credit_type: CASE WHEN fcp is NULL THEN NULL ELSE fcp.credittypec END ");
                sb.append("               , ob: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditortypeotherc END ");
                sb.append("               , creditmode_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditmodec END ");
                sb.append("               , creditmode_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditmodeupdate END ");
                sb.append("               , cash_amount_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditcashrecievedquantumc END ");
                sb.append("               , cash_amount_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditcashpaybackexpectedu END ");
                sb.append("               , cash_payback_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditcashpaybackexpectedc END ");
                sb.append("               , cash_payback_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditcashpaybackactualu END ");
                sb.append("               , input_payback_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditproducepaybackexpectedc END ");
                sb.append("               , input_payback_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditproducepaybackquantumu END ");
                sb.append("               , seed_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditseedc END ");
                sb.append("               , seed_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditseedu END ");
                sb.append("               , fertilizer_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditinorganicfertilizerc END ");
                sb.append("               , fertilizer_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditinorganicfertilizer END ");
                sb.append("               , preplanh_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditpreplanherbicidec END ");
                sb.append("               , preplanh_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditpreplanherbicide END ");
                sb.append("               , postplanh_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditpostplanherbicidec END ");
                sb.append("               , postplanh_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditpostplanherbicide END ");
                sb.append("               , plough_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditploughingc END ");
                sb.append("               , plough_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditploughing END ");
                sb.append("               , handling_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditthreshing END ");
                sb.append("               , handling_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.credithreshingc END ");
                sb.append("               , transport_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.credittransportc END ");
                sb.append("               , transport_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.credittransport END ");
                sb.append("               , storage_planned: CASE WHEN fcp is NULL THEN NULL ELSE fcp.creditproducestoragec END ");
                sb.append("               , storage_actual: CASE WHEN fcpu is NULL THEN NULL ELSE fcpu.creditproducestorage END}) ");
                sb.append("} as info ");
                cql = sb.toString();
                break;
            default: cql = "";
        }
        return cql;
    }

    private SQLRowObj getSQLRowObj(String data_set, Object row, int count) {
        SQLRowObj y = null;
        JSONObject x = getJSONFromRow(row, count);

        if (x != null) {
            y = new SQLRowObj();

            if (data_set.equals(DATA_SET_AGENT)) {
                String code = getJSONValue(x, "", "code", "");
                String lmd = getJSONValue(x, "", "lmd", String.valueOf(DEFAULT_TIME));
                ArrayList<Integer> yMS = (code.startsWith("AG"))
                    ? getDateFromCode(code) : getDateFromTime(Long.valueOf(lmd));

                y.partner = getJSONValue(x, "", "partner", "");
                y.year = yMS.get(0);
                y.month = yMS.get(1);

            } else if (data_set.equals(DATA_SET_COMMUNITY)) {
                String lmd = getJSONValue(x, "", "lmd", String.valueOf(DEFAULT_TIME));
                ArrayList<Integer> yMS = getDateFromTime(Long.valueOf(lmd));

                y.community = getJSONValue(x, "", "community","");
                y.year = yMS.get(0);
                y.month = yMS.get(1);

            } else if (data_set.equals(DATA_SET_FARMER)) {
                String lmd = getJSONValue(x, "time", "lmd", String.valueOf(DEFAULT_TIME));
                ArrayList<Integer> yMS = getDateFromTime(Long.valueOf(lmd));

                y.partner = getJSONValue(x, "", "partner","");
                y.community = getJSONValue(x, "demo", "community","");
                y.year = yMS.get(0);
                y.month = yMS.get(1);

            } else if (data_set.equals(DATA_SET_FARM)) {
                String lmd = getJSONValue(x, "time", "lmd", String.valueOf(DEFAULT_TIME));
                ArrayList<Integer> yMS = getDateFromTime(Long.valueOf(lmd));

                y.farmer_id = getJSONValue(x, "", "farmerId","");
                y.agent_id = getJSONValue(x, "", "agent","");
                y.partner = getJSONValue(x, "", "partner","");

                y.trial = getJSONValue(x, "bc", "trial","0");
                y.rc = getJSONValue(x, "bc", "rc","0");
                y.preph = getJSONValue(x, "bc", "preph","0");
                y.postph = getJSONValue(x, "bc", "postph","0");
                y.inorg = getJSONValue(x, "bc", "inorg","0");
                y.thresh = getJSONValue(x, "bc", "thresh","0");

                y.age = getJSONValue(x, "demo", "age", "0");
                y.gender = getJSONValue(x, "demo", "gender", "unknown");
                if (!y.gender.equals("unknown")) { y.gender = (y.gender.toLowerCase().contains("female")) ? "Female" : "Male"; }
                y.community = getJSONValue(x, "demo", "community","");
                y.location = getJSONValue(x, "demo", "location", "0");

                y.month = yMS.get(1);
                y.year = yMS.get(0); //Integer.valueOf(getJSONValue(x, "time", "year", String.valueOf(START_YEAR)));
                y.season = getJSONValue(x, "time", "season", "Major Season");

                y.crop = getJSONValue(x, "crop", "crop", "");
                y.price_planned = getJSONValue(x, "crop", "price_planned", "0");
                y.price_actual = getJSONValue(x, "crop", "price_actual", "0");
                y.yield_planned = getJSONValue(x, "crop", "yield_planned", "0");
                y.yield_actual = getJSONValue(x, "crop", "yield_actual", "0");

                y.acres_planned = getJSONValue(x, "farminfo", "acres_planned", "0");
                y.acres_actual = getJSONValue(x, "farminfo", "acres_actual", "0");
                y.latitude = getJSONValue(x, "farminfo", "lat", "0");
                y.longitude = getJSONValue(x, "farminfo", "long", "0");
                y.pp = getJSONValue(x, "farminfo", "pp", "0");
                y.fmp = getJSONValue(x, "farminfo", "fmp", "0");
                y.fmp_update = getJSONValue(x, "farminfo", "fmp_update", "0");
                y.measured = getJSONValue(x, "farminfo", "measured", "0");
                y.assessed = getJSONValue(x, "farminfo", "assessed", "0");

                y.credit_planned = getJSONValue(x, "farmcreditplan", "credit_planned", "0");
                y.credit_planned = (y.credit_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.credit_actual = getJSONValue(x, "farmcreditplan", "credit_actual", "0");
                y.credit_actual = (y.credit_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.credit_type = getJSONValue(x, "farmcreditplan", "credit_type", "");
                y.ob = getJSONValue(x, "farmcreditplan", "ob", "0");
                y.ob = (y.ob.equalsIgnoreCase("OB")) ? "1" : "0";

                String cmode_planned = getJSONValue(x, "farmcreditplan", "creditmode_planned", "0");
                String cmode_actual = getJSONValue(x, "farmcreditplan", "creditmode_actual", "0");
                y.cash_planned = (cmode_planned.toLowerCase().contains("cash")) ? "1" : "0";
                y.cash_actual = (cmode_actual.toLowerCase().contains("cash")) ? "1" : "0";
                y.input_planned = (cmode_planned.toLowerCase().contains("inputs")) ? "1" : "0";
                y.input_actual = (cmode_actual.toLowerCase().contains("inputs")) ? "1" : "0";

                y.cash_amount_planned = getJSONValue(x, "farmcreditplan", "cash_amount_planned", "0");
                y.cash_amount_actual = getJSONValue(x, "farmcreditplan", "cash_amount_actual", "0");
                y.cash_payback_planned = getJSONValue(x, "farmcreditplan", "cash_payback_planned", "0");
                y.cash_payback_actual = getJSONValue(x, "farmcreditplan", "cash_payback_actual", "0");
                y.input_payback_planned = getJSONValue(x, "farmcreditplan", "input_payback_planned", "0");
                y.input_payback_actual = getJSONValue(x, "farmcreditplan", "input_payback_actual", "0");

                y.seed_planned = getJSONValue(x, "farmcreditplan", "seed_planned", "0");
                y.seed_actual = getJSONValue(x, "farmcreditplan", "seed_actual", "0");
                y.seed_planned = (y.seed_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.seed_actual = (y.seed_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.fertilizer_planned = getJSONValue(x, "farmcreditplan", "fertilizer_planned", "0");
                y.fertilizer_actual = getJSONValue(x, "farmcreditplan", "fertilizer_actual", "0");
                y.fertilizer_planned = (y.fertilizer_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.fertilizer_actual = (y.fertilizer_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.preplanh_planned = getJSONValue(x, "farmcreditplan", "preplanh_planned", "0");
                y.preplanh_actual = getJSONValue(x, "farmcreditplan", "preplanh_actual", "0");
                y.preplanh_planned = (y.preplanh_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.preplanh_actual = (y.preplanh_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.postplanh_planned = getJSONValue(x, "farmcreditplan", "postplanh_planned", "0");
                y.postplanh_actual = getJSONValue(x, "farmcreditplan", "postplanh_actual", "0");
                y.postplanh_planned = (y.postplanh_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.postplanh_actual = (y.postplanh_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.plough_planned = getJSONValue(x, "farmcreditplan", "plough_planned", "0");
                y.plough_actual = getJSONValue(x, "farmcreditplan", "plough_actual", "0");
                y.plough_planned = (y.plough_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.plough_actual = (y.plough_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.handling_planned = getJSONValue(x, "farmcreditplan", "handling_planned", "0");
                y.handling_actual = getJSONValue(x, "farmcreditplan", "handling_actual", "0");
                y.handling_planned = (y.handling_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.handling_actual = (y.handling_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.transport_planned = getJSONValue(x, "farmcreditplan", "transport_planned", "0");
                y.transport_actual = getJSONValue(x, "farmcreditplan", "transport_actual", "0");
                y.transport_planned = (y.transport_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.transport_actual = (y.transport_actual.toLowerCase().contains("yes")) ? "1" : "0";

                y.storage_planned = getJSONValue(x, "farmcreditplan", "storage_planned", "0");
                y.storage_actual = getJSONValue(x, "farmcreditplan", "storage_actual", "0");
                y.storage_planned = (y.storage_planned.toLowerCase().contains("yes")) ? "1" : "0";
                y.storage_actual = (y.storage_actual.toLowerCase().contains("yes")) ? "1" : "0";
            }
        }
        return y;
    }

    private static JSONObject getJSONFromRow(Object row, int count) {
        try {
            String rowAsString = row.toString();
            rowAsString = rowAsString.replace("=" , "\":\"").replace("," , "\",").replace("[" , "").replace("]" , "");
            rowAsString = rowAsString.replace("{" , "{\"").replace("}" , "\"}").replace(", " , ", \"");
            rowAsString = rowAsString.replace("\"{" , "{").replace("}\"" , "}");
            rowAsString = rowAsString.replace("\"null\"" , "null");
            String attr = "duplicate"+count;
            rowAsString = rowAsString.replace(" {"," "+attr+":{");
            return new JSONObject(rowAsString);
        } catch (JSONException e) {
            System.out.println("Row object: " + row.toString());
            e.printStackTrace();
        }

        return null;
    }

    private static String getJSONValue(JSONObject x, String parent, String child, String defValue) {
        String v=defValue;

        if (!parent.equals("")) {
            try {
                if (x.has(parent)) {
                    JSONObject o = x.getJSONObject(parent);
                    if (o != null) {
                        return getJSONValue(o, "" , child, defValue);
                    }
                } else {
                    v = defValue;
                }
            } catch(Exception e) {
                v=defValue;
            }
        } else {
            try {
                if (x.has(child)) {
                    v = (x.get(child) == null) ? defValue : (String) x.get(child);
                } else {
                    v = defValue;
                }
            } catch (Exception e) {
                v = defValue;
            }
        }

        return v;
    }

    private HashMap<String, SQLRowObj> updateHashMap(HashMap<String, SQLRowObj> data, String key, SQLRowObj object) {
        SQLRowObj obj = object;
        if (data.containsKey(key)) {
            data.get(key).actual++;
        } else {
            obj.actual = 1;
            data.put(key, obj);
        }
        return data;
    }

    private void runUpdates(String tableName, HashMap<String, SQLRowObj> data) throws Exception {
        try {
            if (data.size() > 0) {
                System.out.println("Running updates for "+tableName);
                DBUtil.getInstance().emptyTable(tableName);
                for (SQLRowObj r : data.values()) {
                    String sql = "INSERT INTO " + tableName + " (" + r.fields(tableName) + ") VALUES " + r.values(tableName);
                    DBUtil.getInstance().runSQLUpdate(sql);
                }
            }
        } catch (SQLException e) {
           
            e.printStackTrace();
             throw(e);
        } catch (Exception e) {
            e.printStackTrace();
            throw(e);
        }
    }

    private static class SQLRowObj {
	   // Common
	    public int year;
	    public int month;
	    public long actual;
	    public String partner;
	    public String community;

	    // Credit
	    public String credit_planned;
	    public String credit_actual;
	    public String credit_type;
	    public String ob;
	    public String cash_planned;
	    public String cash_actual;
	    public String cash_amount_planned;
	    public String cash_amount_actual;
	    public String cash_payback_planned;
	    public String cash_payback_actual;
	    public String input_planned;
	    public String input_actual;
	    public String input_payback_planned;
	    public String input_payback_actual;
	    public String seed_planned;
	    public String seed_actual;
	    public String fertilizer_planned;
	    public String fertilizer_actual;
	    public String preplanh_planned;
	    public String preplanh_actual;
	    public String postplanh_planned;
	    public String postplanh_actual;
	    public String plough_planned;
	    public String plough_actual;
	    public String handling_planned;
	    public String handling_actual;
	    public String transport_planned;
	    public String transport_actual;
	    public String storage_planned;
	    public String storage_actual;

	    // Farm element
	    public String acres_planned;
	    public String acres_actual;
	    public String yield_actual;
	    public String yield_planned;
	    public String price_actual;
	    public String price_planned;
	    public String crop;
	    public String season;
	    public String agent_id;
	    public String farmer_id;
	    public String gender;
	    public String age;
	    public String latitude;
	    public String longitude;
	    public String location;
	    public String pp;
	    public String fmp;
	    public String fmp_update;
	    public String measured;
	    public String assessed;

        // Behaviour change
        public String trial;
        public String rc;
        public String preph;
        public String postph;
        public String inorg;
        public String thresh;

	    public String get_year() { return String.valueOf(year); }
	    public String get_month() { return String.valueOf(month); }
	    public String get_actual() { return String.valueOf(actual); }
	    public String get_community() { return community; }
	    public String get_partner() { return partner; }

        public String get_trial() { return String.valueOf(trial); }
        public String get_rc() { return String.valueOf(rc); }
        public String get_preph() { return String.valueOf(preph); }
        public String get_postph() { return String.valueOf(postph); }
        public String get_inorg() { return String.valueOf(inorg); }
        public String get_thresh() { return String.valueOf(thresh); }

	    public String get_acres_planned() { return String.valueOf(acres_planned); }
	    public String get_acres_actual() { return String.valueOf(acres_actual); }
	    public String get_yield_planned() { return String.valueOf(yield_planned); }
	    public String get_yield_actual() { return String.valueOf(yield_actual); }
	    public String get_price_planned() { return String.valueOf(price_planned); }
	    public String get_price_actual() { return String.valueOf(price_actual); }
	    public String get_crop() { return String.valueOf(crop); }
	    public String get_season() { return String.valueOf(season); }
	    public String get_agent_id() { return String.valueOf(agent_id); }
	    public String get_farmer_id() { return String.valueOf(farmer_id); }
	    public String get_gender() { return String.valueOf(gender); }
	    public String get_age() { return String.valueOf(age); }
	    public String get_latitude() { return String.valueOf(latitude); }
	    public String get_longitude() { return String.valueOf(longitude); }
	    public String get_location() { return String.valueOf(location); }
	    public String get_pp() { return String.valueOf(pp); }
	    public String get_fmp() { return String.valueOf(fmp); }
	    public String get_fmp_update() { return String.valueOf(fmp_update); }
	    public String get_measured() { return String.valueOf(measured); }
	    public String get_assessed() { return String.valueOf(assessed); }

	    public String get_credit_planned() { return String.valueOf(credit_planned); }
	    public String get_credit_actual() { return String.valueOf(credit_actual); }
	    public String get_credit_type() { return String.valueOf(credit_type); }
	    public String get_ob() { return String.valueOf(ob); };
	    public String get_cash_planned() { return String.valueOf(cash_planned); }
	    public String get_cash_actual() { return String.valueOf(cash_actual); }
	    public String get_cash_amount_planned() { return String.valueOf(cash_amount_planned); }
	    public String get_cash_amount_actual() { return String.valueOf(cash_amount_actual); }
	    public String get_cash_payback_planned() { return String.valueOf(cash_payback_planned); }
	    public String get_cash_payback_actual() { return String.valueOf(cash_payback_actual); }
	    public String get_input_planned() { return String.valueOf(input_planned); }
	    public String get_input_actual() { return String.valueOf(input_actual); }
	    public String get_input_payback_planned() { return String.valueOf(input_payback_planned); }
	    public String get_input_payback_actual() { return String.valueOf(input_payback_actual); }
	    public String get_seed_planned() { return String.valueOf(seed_planned); }
	    public String get_seed_actual() { return String.valueOf(seed_actual); }
	    public String get_fertilizer_planned() { return String.valueOf(fertilizer_planned); }
	    public String get_fertilizer_actual() { return String.valueOf(fertilizer_actual); }
	    public String get_preplanh_planned() { return String.valueOf(preplanh_planned); }
	    public String get_preplanh_actual() { return String.valueOf(preplanh_actual); }
	    public String get_postplanh_planned() { return String.valueOf(postplanh_planned); }
	    public String get_postplanh_actual() { return String.valueOf(postplanh_actual); }
	    public String get_plough_planned() { return String.valueOf(plough_planned); }
	    public String get_plough_actual() { return String.valueOf(plough_actual); }
	    public String get_handling_planned() { return String.valueOf(handling_planned); }
	    public String get_handling_actual() { return String.valueOf(handling_actual); }
	    public String get_transport_planned() { return String.valueOf(transport_planned); }
	    public String get_transport_actual() { return String.valueOf(transport_actual); }
	    public String get_storage_planned() { return String.valueOf(storage_planned); }
	    public String get_storage_actual() { return String.valueOf(storage_actual); }

	    public String id(String tableName) {
	        String id = "";
	        String common = get_year() + "-" + get_month();
	        switch(tableName) {
	            case TABLE_AGENT: id = get_partner() + "-" + common; break;
	            case TABLE_COMMUNITY: id = common; break;
	            case TABLE_FARMER: id = get_community().toLowerCase() + "-" + get_partner() + "-" + common; break;
	            case TABLE_FARM: id = get_farmer_id() + "-" + get_season() + "-" + common; break;
	        }
	        return id;
	    }

	    public String values(String tableName) throws Exception {
	        try {
	            String[] f = (fields(tableName)).split(",");
	            String values = "(";
	            for (int i=0; i < f.length; i++) {
	                boolean addComma = (i < f.length-1);
	                values += q(callMethod("get_" + f[i].trim()), addComma);
	            }
	            values += ");";
	            return values;
	        } catch (Exception e) {
	            System.out.println("Error getting values for "+tableName+": "+e.getLocalizedMessage());
	            e.printStackTrace();
	            throw(e);
	        }
	    }

	    public String fields(String tableName) {
	        String time = "year, month";
	        String common = "actual, " + time;
	        String partner = ", partner";
	        String community = ", community";

	        String farmMeta = ", acres_planned, acres_actual, yield_planned, yield_actual"
	                + ", price_planned, price_actual, crop, season"
	                + ", partner, agent_id, farmer_id"
	                + ", gender, age, latitude, longitude, location, pp, fmp, fmp_update, measured, assessed"
	                + ", credit_planned , credit_actual, credit_type, ob , cash_planned , cash_actual , cash_amount_planned, cash_amount_actual"
	                + ", cash_payback_planned, cash_payback_actual, input_planned , input_actual , input_payback_planned, input_payback_actual"
	                + ", seed_planned, seed_actual, fertilizer_planned, fertilizer_actual, preplanh_planned, preplanh_actual"
	                + ", postplanh_planned, postplanh_actual, plough_planned, plough_actual, handling_planned, handling_actual"
	                + ", transport_planned, transport_actual, storage_planned, storage_actual"
                    + ", trial, rc, preph, postph, inorg, thresh";

	        String fields = common;

	        switch(tableName) {
	            case TABLE_AGENT: fields = fields + partner; break;
	            case TABLE_FARMER: fields = fields + partner + community; break;
	            case TABLE_FARM: fields = time + farmMeta; break;
	        }

	        return fields;
	    }

	    private String callMethod(String methodName) throws Exception {
	        try { return (String) this.getClass().getMethod(methodName).invoke(this); }
	        catch (SecurityException e) { throw(e); }
	        catch (NoSuchMethodException e) { throw(e); }
	        catch (IllegalArgumentException e) { throw(e); }
	        catch (IllegalAccessException e) { throw(e); }
	        catch (InvocationTargetException e) { throw(e); }
	    }

	    private String q(String val, boolean comma) { return "'" + val + "'" + ((comma) ? ", ":""); }
    }
    // </editor-fold>
}
