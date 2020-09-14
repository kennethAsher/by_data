package com.bangying.cleandata;


import com.bangying.utils.PatternUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PublicExpense {
    //    打印日志
    private final static Logger logger = LoggerFactory.getLogger(Doc.CaseParty.class);
    //    去掉括号已经括号里面的内容
    private final static Pattern remove_parent = Pattern.compile("[\\(（].*?[\\)）]");
    //    检测是否减半收取
    private final static Pattern havle_accept_pat = Pattern.compile("减半");
    //    费用类型
    private final static Pattern cost_type_pat = Pattern.compile("(保全保险费|评估费|反诉受理费|诉讼保全费|财产保全费|保全费|公告费)(.*?)元");
    //    未知费用类型
    private final static Pattern cost_no_pat = Pattern.compile("费(.*?)元");
    //    判断是否由附加费用，合计
    private final static Pattern merge_pat = Pattern.compile("费(.*?)元(.*?)计(.*?)元");
    //    承担人员
    private final static Pattern person_pat = Pattern.compile("(原审原告|原审被告|原告人|被告人|原告|被告|被上诉人|上诉人|原审第三人|第三人|被申诉人|申诉人|再审申请人|申请再审人|被申请人|申请人|申请执行人|被执行人|被异议人|异议人|起诉人|申报人)(.*?)(自行负担|自行承担|共同负担|承担|负担|担负|均担|担)(.*?)元");
    private final static Pattern person_pat1 = Pattern.compile("(原审原告|原审被告|原告人|被告人|原告|被告|被上诉人|上诉人|原审第三人|第三人|被申诉人|申诉人|再审申请人|申请再审人|被申请人|申请人|申请执行人|被执行人|被异议人|异议人|起诉人|申报人)(自行负担|自行承担|共同负担|承担|负担|担负|均担|担)(.*?)元");
    private final static Pattern person_pat2 = Pattern.compile("[由]?(.*?)(自行负担|自行承担|共同负担|承担|负担|担负|均担|担)(.*?)元");
    private final static Pattern person_pat3 = Pattern.compile("[由]?(原审原告|原审被告|原告人|被告人|原告|被告|被上诉人|上诉人|原审第三人|第三人|被申诉人|申诉人|再审申请人|申请再审人|被申请人|申请人|申请执行人|被执行人|被异议人|异议人|起诉人|申报人)(.*?)(自行负担|自行承担|共同负担|承担|负担|担负|均担|担)");
    private final static Pattern person_pat4 = Pattern.compile("[由]?(.*?)(自行负担|自行承担|共同负担|承担|负担|担负|均担|担)");
    //    切分语句的正则
    private final static Pattern doc_split_pat = Pattern.compile("。");
    //    需要替换的内容：&#xa0; 。。
    private final static Pattern doc_clean_pat = Pattern.compile("。。|&#xa0;");
    //    判断是否存在可挖去的价值
    private final static Pattern doc_accept_coss_pat = Pattern.compile(".*受理费[用]?[:：]?(.*)元.*");
    //    受理费用的清洗正则   受理费[费本院实际全额依法预缴应收用共计由已因适用按简易普通程序（(减半)收取）计算后交缴纳征收取计即为合计人民币元各到]{0,20}   [元减半收取0-9]{0,10}[元]?
    private final static Pattern accept_cost_pat = Pattern.compile("(\\d+(\\.\\d+)?[万]?)");
    //    是否符合受理费所在的句子
    private final static Pattern accept_line_pat = Pattern.compile(".*受理费.*([担负])");

    public PublicExpense() {
    }

    //    清洗任务的总程序
    public static JavaRDD<String> cleanPublicExpense(JavaRDD<String> file_rdd) {
        logger.info("开始清洗官费");
        JavaRDD<String> lines_rdd = file_rdd.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                StringBuffer doc = new StringBuffer(PatternUtils.sub(s, doc_clean_pat, ""));
                StringBuffer id = new StringBuffer(doc.toString().split("\\|")[0]);
                String[] lines = doc_split_pat.split(doc);
                StringBuffer bear_line = null;     //承担者的数据行
                StringBuffer accept_cost = null;   //受理费
                StringBuffer amount = null;        //标的额
                for (String line : lines) {
//                    判断是否符合清洗官费的条件
                    if (PatternUtils.existPattern(line, accept_line_pat)) {
//                        得到承担费用的详情
                        try {
                            bear_line = new StringBuffer(StringUtils.join(Arrays.copyOfRange(line.split("，"), 1, line.split("，").length - 1), "，"));
                        }catch(Exception e){
                            bear_line = new StringBuffer(line);
                        }
                        accept_cost = new StringBuffer((getAcceptCost(line).equals("空"))?"0":getAcceptCost(line));
                        amount = new StringBuffer(acceptCost2SubjectCost(accept_cost.toString()));
                        break;
                    }
                }
                try{
                    assert bear_line != null;
                    if ((bear_line.toString().length() > 2)) {
                        Map<String, String> result_map = cleanLine(accept_cost.toString(), bear_line.toString());
                        return id+"|标的额-"+amount+"|受理费-"+accept_cost+"|"+bear_line+"|"+result_map.get("cost_line")+"|"+result_map.get("person");
                    } else {
                        return "-";
                    }
                } catch (Exception e){
                    return "-";
                }


            }
        });
        return lines_rdd.filter(line->!line.equals("-"));
    }

    //    清洗出承担人
    public static String cleanPerson(String accept_cost, String cost_line, String line) {
        /*
         * @Author      kennethAsher
         * @Date        5:12 下午 2020/9/9
         * @param accept_cost   受理费用
         * @param cost_line     受理费用所在的数据行
         * @param line          整个数据行
         * @return      java.lang.String  返回承担的人物
         * @Description //TODO 清洗出原告或者被告具体由那一方承担费用
         */
        if (cost_line.endsWith("均")) {
            cost_line = cost_line.replace("均", "");
        }
        //    判断是否符合承担人员的正则，并分别处理
        if (person_pat.matcher(line).find()) {
            return line;
        } else if (person_pat1.matcher(line).find()) {
            return line;
        } else if (person_pat2.matcher(line).find()) {
            return line;
        } else if (person_pat3.matcher(line).find()) {
            //    判断是否有额外费用，判断长度，满足意味者没有，直接返回承担着+受理费用
            if (cost_line.length() < 2) {
                return line + accept_cost + "元";
            }
//            是否存在额外的费用
            if (merge_pat.matcher(cost_line).find()) {
                return line + PatternUtils.findAll(cost_line, merge_pat).get(PatternUtils.findAll(cost_line, merge_pat).size() - 1) + "元";
            }
//            是否存在减半收取
            if (cost_line.contains("减半")) {
                return line + Float.toString(Float.parseFloat(accept_cost) / 2) + "元";
            }
        } else if (person_pat4.matcher(line).find()) {
            if (cost_line.length() < 2) {
                return line + accept_cost + "元";
            }
            if (merge_pat.matcher(cost_line).find()) {
                return line + PatternUtils.findAll(cost_line, merge_pat).get(PatternUtils.findAll(cost_line, merge_pat).size() - 1) + "元";
            }
            if (cost_line.contains("减半")) {
                return line + Float.toString(Float.parseFloat(accept_cost) / 2) + "元";
            }
            return line + accept_cost + "元";
        } else {
            return line;
        }
        return line;
    }

    //    清洗官费
    public static String getAcceptCost(String line) {
        /*
         * @Author      kennethAsher
         * @Date        4:48 下午 2020/9/8
         * @param       line    带有受理费用的数据行
         * @return      java.lang.String    返回清理好的受理费用等
         * @Description //TODO 将带有受理费的数据行的具体受理费金额清洗出来
         */
        line = line.replace(",", "").replace("，", "");
        List<String> money_list = PatternUtils.findAll(line, accept_cost_pat);
        if (money_list.size() > 0) {
            if (money_list.get(0).contains("万")) {
                return Float.toString(Float.parseFloat(money_list.get(0).replace("万", "")) * 10000);
            }
            return money_list.get(0);
        }
        return "空";
    }

    //    计算标的额
    public static String acceptCost2SubjectCost(String cost) {
        /*
         * @Author kennethAsher
         * @Description //TODO 将受理费用倒推产生标的额并返回
         * @Date 4:16 下午 2020/9/8
         * @param cost  受理费用（String）类型
         * @return java.lang.String  标的额
         **/
//        判断是否符合转换，不符合直接返回空，符合继续进行
        if (cost.contains("空") || cost.equals("")) {
            return "空";
        }
        float money = Float.parseFloat(cost);
//        根据不同的受理费计算出标的额并且返回
        if (money <= 50) {
            return "10000.00";
        } else if (money < 2300) {
            return String.format("%.2f", (float) ((money - 50) / 0.025 + 10000));
        } else if (money <= 4300) {
            return String.format("%.2f", (float) ((money - 2300) / 0.02 + 100000));
        } else if (money <= 8800) {
            return String.format("%.2f", (float) ((money - 4300) / 0.015 + 200000));
        } else if (money <= 13800) {
            return String.format("%.2f", (float) ((money - 8800) / 0.01 + 500000));
        } else if (money <= 22800) {
            return String.format("%.2f", (float) ((money - 13800) / 0.009 + 1000000));
        } else if (money <= 46800) {
            return String.format("%.2f", (float) ((money - 22800) / 0.008 + 2000000));
        } else if (money <= 81800) {
            return String.format("%.2f", (float) ((money - 46800) / 0.007 + 5000000));
        } else if (money <= 141800) {
            return String.format("%.2f", (float) ((money - 81800) / 0.006 + 10000000));
        } else if (money > 141800) {
            return String.format("%.2f", (float) ((money - 141800) / 0.005 + 20000000));
        } else {
            return "空";
        }
    }

    //    将费用分类，承担者，承担金额返回
    public static Map<String, String> cleanLine(String accept_cost, String line) {
        /*
         * @Author      kennethAsher
         * @Date        5:32 下午 2020/9/9
         * @param       accept_cost   受理费用
         * @param       line          受理费所在行数据
         * @return      java.util.Map<java.lang.String,java.lang.String>
         * @Description //TODO        将费用分类，和承担人员返回
         */
        line = PatternUtils.sub(line, remove_parent, "");
        String[] words;
        String cost_line = "";
        String person_line = line;
//        下列判断是否需要处理掉
        if (line.contains("如下")) {
            words = line.split("案件受理");
            line = "案件受理" + words[words.length - 1];
        }
        if (line.contains("不服")) {
            if (line.contains("如不服")) {
                line = cleanLineUtils("如不服", line);
            } else {
                line = cleanLineUtils("不服", line);
            }
        }
        if (line.contains("审判")) {
            line = cleanLineUtils("审判", line);
        }
        if (line.contains("判决") || line.contains("生效")) {
            words = line.split("，");
            line = line.replace(words[words.length - 1], "").substring(0, line.replace(words[words.length - 1], "").length() - 2);
        }
        if (line.contains("由")) {
            int index = line.indexOf("由");
            cost_line = line.substring(0, index);
            person_line = line.substring(index, line.length() - 1);
        }
        String person = cleanPerson(accept_cost, cost_line, person_line);
        Map<String, String> map = new HashMap<String, String>();
        map.put("cost_line", cost_line);
        map.put("person", person);
        return map;
    }

    //    工具方法，主要是去掉cleanLine方法中大量重复的if-else
    public static String cleanLineUtils(String word, String line) {
        String[] words = line.split(word);
        line = line.replace(word + words[words.length - 1], "");
        return line;
    }
}
