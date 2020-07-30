package com.bangying.cleandata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.ForeachFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * ClassName:      SupplementAddData
 * Package:        com.bangying.cleandata
 * Datetime:       2020/7/28   10:18 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:
 */
public class SupplementAddData implements ForeachFunction<Row> {

    private static final long serialVersionUID = 1891711790350746873L;
    private static Pattern pattern = Pattern.compile("NULL|\\+|,|-|\\.|0|1|2|3|4|5|6|7|8|9|>|\\?|null|VF|_|`|·|ˎ̥|‘|、|【");
    private static Map<String, String> map = new HashMap<String, String>();

    public SupplementAddData() {

    }

    //    补充函数实现，缺少函数也能实现次方法
    public static JavaRDD<String> updateData(JavaRDD<String> RDD) throws IOException {
        return run(RDD);
    }

    //    主要函数，用于切分补充原文件
    private static JavaRDD<String> run(JavaRDD<String> java_rdd) throws IOException{
        JavaRDD<String> lines_rdd = java_rdd.map(new Function<String, String>() {
            private static final long serialVersionUID = 1L;
            //  开始处理每行的数据,注意不能返回null，否则会报错，应该返回一个数值，例如"-"，在过滤掉即可
            public String call(String line) throws IOException {
                String[] fields = line.split("\\|");
                if (fields.length < 7) {
                    return "-";
                }
                String doc_id = fields[0];
                String title = fields[1];
                String court = getCleanCourt(fields[2], pattern);
                String case_number = cleanCaseNumber(fields[4]);
                String trail_name = cleanTrialName(case_number);
                String year = (fields[5].length() > 2) ? fields[5] : "";
                String date = (fields[6].length() > 5) ? fields[6] : "";
                Map<String, String> type_map = getTypes(title, fields[3], map);
                String case_type = type_map.get("case_type");
                String doc_type = type_map.get("doc_type");
                return doc_id+"|"+title+"|"+court+"|"+case_number+"|"+year+"|"+date+"|"+trail_name+"|"+case_type+"|"+doc_type;
            }
        });
        JavaRDD<String> out_rdd = lines_rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s=="-" ? false : true;
            }
        });
        return out_rdd;
    }

    //    清洗案号，统一将括号替换成英文括号
    public static String cleanCaseNumber(String caseNumber) {
        caseNumber = caseNumber.replace('（', '(')
                .replace('）', ')')
                .replace('〔', '(')
                .replace('〕', ')');
        return caseNumber;
    }

    //    返回分好类的审理程序,通过案号含有的字眼判断是否符合
    public static String cleanTrialName(String case_name) {
        if (case_name.contains("初")) {
            return "一审";
        } else if (case_name.contains("终")) {
            return "二审";
        } else if (case_name.contains("特监") || (case_name.contains("督监"))) {
            return "特殊程序";
        } else if (case_name.contains("再")) {
            return "再审";
        }
        return "特殊程序";
    }

    //    清洗法院自带的不正确的格式
    public static String getCleanCourt(String court_name, Pattern pattern) {
        try {
            String[] court_list = pattern.split(court_name);
            String court = court_list[court_list.length - 1];
            if (court.length() < 4) {
                return "";
            }
            return court;
        } catch (Exception e) {
            System.out.println("错误的法院数据"+court_name);
            return "";
        }
    }

    //    通过源数据的判决书字段和标题字段得到案件类型和文书类型
    public static Map<String, String> getTypes(String title, String type_name, Map<String, String> map) {
        if (type_name.contains("刑事")) {
            map.put("case_type", "刑事案件");
            if (title.contains("附带")) {
                map.put("doc_type", "刑事附带民事判决书");
            } else if (title.contains("判决")) {
                map.put("doc_type", "刑事判决书");
            } else if (title.contains("裁定")) {
                map.put("doc_type", "刑事裁定书");
            } else if (title.contains("死刑")) {
                map.put("doc_type", "执行死刑命令");
            } else {
                map.put("doc_type", "");
            }
        } else if (type_name.contains("行政")) {
            map.put("case_type", "行政案件");
            if (title.contains("附带")) {
                map.put("doc_type", "行政附带民事判决书");
            } else if (title.contains("赔偿判决")) {
                map.put("doc_type", "行政赔偿判决书");
            } else if (title.contains("判决")) {
                map.put("doc_type", "行政判决书");
            } else if (title.contains("裁定")) {
                map.put("doc_type", "行政裁定书");
            } else if (title.contains("调解")) {
                map.put("doc_type", "行政赔偿调解书");
            } else {
                map.put("doc_type", "");
            }
        } else if (type_name.contains("民事")) {
            map.put("case_type", "民事案件");
            if (title.contains("判决")) {
                map.put("doc_type", "民事判决书");
            } else if (title.contains("裁定")) {
                map.put("doc_type", "民事裁定书");
            } else if (title.contains("决定")) {
                map.put("doc_type", "民事决定书");
            } else if (title.contains("调解")) {
                map.put("doc_type", "民事调解书");
            } else {
                map.put("doc_type", "");
            }
        } else if (type_name.contains("执行")) {
            map.put("case_type", "执行案件");
            if (title.contains("裁定")) {
                map.put("doc_type", "执⾏裁定书");
            } else {
                map.put("doc_type", "");
            }
        } else if (title.contains("赔偿")) {
            map.put("case_type", "执行案件");
            map.put("doc_type", "");
        } else {
            map.put("case_type", "");
            map.put("doc_type", "");
        }
        return map;
    }
    //    自带方法必须实现，否则报错
    public void call(Row row) throws Exception {
    }
}
