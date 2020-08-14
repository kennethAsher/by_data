package com.bangying.cleandata;

import com.bangying.utils.Constant;
import com.bangying.utils.HdfsUtils;
import com.bangying.utils.PatternUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
    public static Pattern split_pattern = Pattern.compile("。");
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
    // 正则匹配符合政府机构的名称
    public static Pattern government_pat = Pattern.compile(".*(委|办公室|局|会|办|政协|部|政府|党校|部朝阳大街|信|中心|委银州区|厅|史馆|科学院|科院|事馆|处)$");

    //4个级别的案由
    public static Set<String> set_one = new HashSet<String>();
    public static Set<String> set_two = new HashSet<String>();
    public static Set<String> set_three = new HashSet<String>();
    public static Set<String> set_four = new HashSet<String>();

    //百家姓和带标签企业的名单
    public static Set<String> set_family_one = new HashSet<String>();
    public static Set<String> set_family_two = new HashSet<String>();
    public static Set<String> set_family_three = new HashSet<String>();
    public static Set<String> set_label = new HashSet<String>();

    public CaseParty() {
    }

    //  返回清洗的
    public static JavaRDD<String> cleanCaseParty(JavaRDD<String> RDD) throws IOException {
        return run(RDD);
    }

    //  开始清洗caseparty的主要运行函数
    public static JavaRDD<String> run(JavaRDD<String> rdd) throws IOException {
        addSet();
//        System.out.println(set_family_one);
//        System.out.println(set_family_two);
//        System.out.println(set_family_three);
//        System.out.println(set_label);
        JavaRDD<String> lines_rdd = rdd.map(new Function<String, String>() {
            @Override
            public String call(String line_mainbody) throws Exception {
                String doc_id;
                String mainbody;
                String[] fields = line_mainbody.split("\\|");
                try {
                    doc_id = fields[0];
                    mainbody = fields[1];
                } catch (Exception e) {
                    return "-";
                }
                //替换掉html文件并且切分
                String[] lines = split_pattern.split(PatternUtils.sub(mainbody, html_remove_pattern, ""));
                if (lines.length < 3) {
                    return doc_id + "||";
                }
                String split_word = "";
                String party_line = "";
                String cause_name = "";
                for (int i = 0; i < lines.length; i++) {
                    // 移除括号内的内容
                    String remove_parent = PatternUtils.sub(lines[i], constant_remove_pattern, "");
                    // 查找所有匹配到的当事人信息
                    List<String> result = PatternUtils.findAll(remove_parent, parties_pattern);
                    if (result.size() > 0 && !lines[i].contains("一案")) {
                        // 当事人类型的列表,索引为0的时候是当前当事人的类型
                        try {
                            String party_type = PatternUtils.findAll(remove_parent, parties_split_pattern).get(0);
                            // 当事人切分
                            String[] split_moves = parties_split_pattern.split(remove_parent);
                            // 获得当事人的姓名
                            String name = PatternUtils.sub(split_moves[1].split("，")[0], clean_party_pattern, "");
                            // 取得当事人，长度大于2，过滤掉（原告母，原告之父此类）
                            if (split_moves[1].length() > 2) {
                                // 取的名字的全程
                                if (name.contains("(")) {
                                    name = name.substring(0, name.indexOf("("));
                                }
                                // 整理公司名称，**公司**分公司
                                if (name.length() > 20 && !name.contains("分公司") && !name.contains("支行")) {
                                    name = name.split("公司")[0] + "公司";
                                }
                                split_word = split_word + "|" + name;
                            }
                            // 将**户几人等畸形词语整理
                            if (name.endsWith("户") && name.length() > 2) {
                                name = name.replace("户", "");
                            }
                            // 切分','取到姓名本体
                            if (name.contains(",")) {
                                name = name.split(",")[0];
                            }
                            // 有的会多余显示被告人张三、李四、王五共同上诉（在前面已经显示完成的情况下） 有的会显示被告人：共同委托代理人XXX
                            if (name.contains("、") || name.contains("委托") || name.contains("诉讼") || name.contains("代理人") || name.contains("��")
                                    || name.contains("Ｘ") || name.contains("职工") || name.length() > 30) {
                                continue;
                            }
                            name = addType(name, set_family_one, set_family_two, set_family_three, set_label);
                            party_line = party_line + "," + party_type + "-" + name;
                        } catch (Exception e) {
                            return doc_id + "||";
                        }
                    }
                    String pat = (split_word.length() < 2) ? "" : split_word.substring(1);
                    party_line = (party_line.startsWith(",")) ? party_line.substring(1) : party_line;
                    if (lines[i].contains("一案")) {
                        try {
                            Pattern split_word_pattern = Pattern.compile("%s".format(pat));
//                            System.out.println("split_word_pattern is " + split_word_pattern);
                            List<String> cause_list = PatternUtils.findAll(remove_parent, cause_pattern);
//                            System.out.println(remove_parent);
                            if (cause_list.size() == 0 || lines[i].contains("姓名或名称")) {
                                cause_name = "";
                            } else {
                                cause_name = split_word_pattern.split(cause_list.get(0))[split_word_pattern.split(cause_list.get(0)).length - 1].replace("一案", "");

//                                cause_name = (cause_name.startsWith("为") && cause_name.endsWith("纠纷")) ? cause_name.substring(1) : cause_name;
//                                cause_name = (!cause_name.contains("��")) ? cleanCauseName(cause_name) : "";
                                cause_name = cleanCauseName(cause_name, set_four, set_three, set_two, set_one);
//                                System.out.println(cause_name);
                            }
                        } catch (Exception e) {
                            if (party_line.length() > 1) {
                                return doc_id + "||" + party_line;
                            } else {
                                return doc_id + "||";
                            }
                        }
                        return doc_id + "|" + cause_name + "|" + party_line;
                    }
                }
                if (party_line.length() < 2) {
                    return doc_id + "|" + cause_name + "|";
                }
                return doc_id + "|" + cause_name + "|" + party_line;
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
    public static void addSet() throws IOException {
        Map<String, Set<String>> set_map = HdfsUtils.getCauseSet();
        set_one = set_map.get("set_one");
        set_two = set_map.get("set_two");
        set_three = set_map.get("set_three");
        set_four = set_map.get("set_four");
        Map<String, Set<String>> label_map = getFamiliesSet();
        set_family_one = label_map.get("family_one");
        set_family_two = label_map.get("family_two");
        set_family_three = label_map.get("family_three");
        set_label = label_map.get("label_set");
    }

    //    清洗案由名称
    public static String cleanCauseName(String cause_name, Set<String> set_4, Set<String> set_3, Set<String> set_2, Set<String> set_1) {
        for (String set4name : set_4) {
            if (set4name.contains(cause_name) || cause_name.contains(set4name)) {
                return set4name;
            }
        }
        for (String set3name : set_3) {
            if (set3name.contains(cause_name) || cause_name.contains(set3name)) {
                return set3name;
            }
        }
        for (String set2name : set_2) {
            if (set2name.contains(cause_name) || cause_name.contains(set2name)) {
                return set2name;
            }
        }
        for (String set1name : set_1) {
            if (set1name.contains(cause_name) || cause_name.contains(set1name)) {
                return set1name;
            }
        }
        return "";
    }

    //    为姓名添加分类
    public static String addType(String name, Set<String> set_family_one, Set<String> set_family_two, Set<String> set_family_three, Set<String> set_label) {
        if (name.length() < 2) {
            return "";
        }
        String clean_name = cleanName(name);
        Matcher matcher = government_pat.matcher(clean_name);
        if (set_label.contains(clean_name)) {
            return clean_name + "-有标签企业";
        } else if (matcher.find()) {
            return clean_name + "-政府";
        } else if ((set_family_one.contains(clean_name.substring(0, 1)) || set_family_two.contains(clean_name.substring(0, 2))
                || set_family_three.contains(clean_name.substring(0, 3))) && clean_name.length() <= 5) {
            return clean_name + "-个人";
        } else {
            return clean_name + "-非上市民营";
        }
    }

    //    清洗带括号的格式不清晰的name名称
    public static String cleanName(String name) {
        if (name.contains("（")) {
            int start_index = name.indexOf("（");
            if (name.contains("）")) {
                int end_index = name.indexOf("）");
                name = name.substring(start_index, end_index) + name.substring(end_index + 1);
            } else {
                name = name.substring(start_index);
            }
        }
        return name;
    }

    //    获取百家姓和有标签企业名称信息
    public static Map<String, Set<String>> getFamiliesSet() throws IOException {
//        定义map返回内容
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
//        定义set接收文件取出的内容
        Set<String> family_one = new HashSet<String>();
        Set<String> family_two = new HashSet<String>();
        Set<String> family_three = new HashSet<String>();
        Set<String> label_set = new HashSet<String>();
//        读取文件位置
        Path family_path = new Path(Constant.getProperty("family_file_path"));
        Path label_path = new Path(Constant.getProperty("label_company_file_path"));
//        开始读取文件
        FileSystem fs = HdfsUtils.getFileSystem();
        BufferedReader family_reader = new BufferedReader(new InputStreamReader(fs.open(family_path)));
        BufferedReader label_reader = new BufferedReader(new InputStreamReader(fs.open(label_path)));
//        循环按行读取文件
        String line;
        while ((line = family_reader.readLine()) != null) {
            if (line.length() == 1) {
                family_one.add(line);
            } else if (line.length() == 2) {
                family_two.add(line);
            } else if (line.length() == 3) {
                family_three.add(line);
            }
        }
        map.put("family_one", family_one);
        map.put("family_two", family_two);
        map.put("family_three", family_three);
        while ((line = label_reader.readLine()) != null) {
            label_set.add(line);
        }
        map.put("label_set", label_set);
        return map;
    }

    //    必须实现的方法，否则会报错
    @Override
    public void call(Row row) throws Exception {

    }
}
