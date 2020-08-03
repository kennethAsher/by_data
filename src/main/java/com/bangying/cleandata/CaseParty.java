package com.bangying.cleandata;

import com.bangying.utils.HdfsUtils;
import com.bangying.utils.PatternUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName:      CaseParty
 * Package:        com.bangying.cleandata
 * Datetime:       2020/7/30   10:44 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    此方法是清洗裁判文书，从之中找到原告，被告以及案由等。（包括诉求方身份）
 */
public class CaseParty implements ForeachFunction<Row> {

    private final static Logger logger = LoggerFactory.getLogger(CaseParty.class);
    // 分行mainbody的正则
    public static Pattern split_pattern = Pattern.compile("。|\\n'");
    // 判断是否符合案由所在句子
    public static Pattern cause_pattern = Pattern.compile("(.*?)一案");
    // 在完整句子中切分出当事人
    public static Pattern name_split_pattern = Pattern.compile("[,，]");
    // 清洗当事人名称
    public static Pattern clean_party_pattern = Pattern.compile("[：?:˙；123456789]");
    // 匹配到当事人
    public static Pattern parties_pattern = Pattern.compile("((原审原告|原审被告|原告人|被告人|原告|被告|被上诉人|上诉人|原审第三人|第三人|被申诉人|申诉人|再审申请人|申请再审人|被申请人|申请人|申请执行人|被执行人|被异议人|异议人|起诉人|申报人)([\\(（].*?[\\)）])?)[:：]?(.*)");
    // 切分当事人
    public static Pattern parties_split_pattern = Pattern.compile("原审原告|原审被告|原告人|被告人|原告|被告|被上诉人|上诉人|原审第三人|第三人|被申诉人|申诉人|再审申请人|申请再审人|被申请人|申请人|申请执行人|被执行人|被异议人|异议人|起诉人|申报人");
    // 清除标签内容
    public static Pattern html_remove_pattern = Pattern.compile(">(.*?)<");
    // 移除括号内的内容
    public static Pattern constant_remove_pattern = Pattern.compile("[\\(（].*?[\\)）]");

    //4个级别的案由
    public static Set<String> set_one = new HashSet<String>();
    public static Set<String> set_two = new HashSet<String>();
    public static Set<String> set_three = new HashSet<String>();
    public static Set<String> set_four = new HashSet<String>();

    public CaseParty() {
    }

    //  返回清洗的
    public static JavaRDD<String> cleanCaseParty(JavaRDD<String> RDD) throws IOException {
        return run(RDD);
    }

    //  开始清洗caseparty的主要运行函数
    public static JavaRDD<String> run(JavaRDD<String> rdd) throws IOException {
        addSet();
        JavaRDD<String> lines_rdd = rdd.map(new Function<String, String>() {
            @Override
            public String call(String line_mainbody) throws Exception {
                String[] fields = line_mainbody.split("\\|");
                String doc_id = fields[0];
                String mainbody = fields[1];
                //替换掉html文件并且切分
                String[] lines = split_pattern.split(PatternUtils.sub(mainbody, html_remove_pattern, ""));
                if (lines.length<3) {return doc_id+"||";}
                String split_word = "";
                String party_line = "";
                for (int i = 0; i < lines.length; i++){
                    // 移除括号内的内容
                    String remove_parent = PatternUtils.sub(lines[i], constant_remove_pattern, "");
                    // 查找所有匹配到的当事人信息
                    List<String> result = PatternUtils.findAll(remove_parent, parties_pattern);
                    if (result.size() > 0 && !lines[i].contains("一案")) {
                        // 当事人类型的列表,索引为0的时候是当前当事人的类型
                        String party_type = PatternUtils.findAll(remove_parent, parties_split_pattern).get(0);
                        // 当事人切分
                        String[] split_moves = parties_split_pattern.split(remove_parent);
                        // 取得当事人，长度大于2，过滤掉（原告母，原告之父此类）
                        if (split_moves[1].length()>2) {
                            // 获得当事人的姓名
                            String name = PatternUtils.sub(split_moves[1].split("，")[0], clean_party_pattern, "");
//                            if (name.contains("(")) {name = name.substring()
                        }
                    }
                }


                return null;
            }
        });

        return lines_rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equals("-");
            }
        });
    }

    //    补充需要用到的set
    public static void addSet() throws IOException{
        Map<String, Set<String>> set_map = HdfsUtils.getCauseSet();
        set_one = set_map.get("set_one");
        set_two = set_map.get("set_two");
        set_three = set_map.get("set_three");
        set_four = set_map.get("set_four");
    }

    //    清洗案由名称
    public static String cleanCauseName(String cause_name) {
        for (String set4name : set_four) {if (set4name.contains(cause_name)||cause_name.contains(set4name)) {return set4name;}}
        for (String set3name : set_three) {if (set3name.contains(cause_name)||cause_name.contains(set3name)) {return set3name;}}
        for (String set2name : set_two) {if (set2name.contains(cause_name)||cause_name.contains(set2name)) {return set2name;}}
        for (String set1name : set_one) {if (set1name.contains(cause_name)||cause_name.contains(set1name)) {return set1name;}}
        return "";
    }

    //    必须实现的方法，否则会报错
    @Override
    public void call(Row row) throws Exception {

    }
}
