package com.bangying.cleandata;
/*
 *   ClassName:      LawyerJudge
 *   Package:        com.bangying.cleandata
 *   Datetime:       2020/8/19   10:27 上午
 *   E-Mail:         1131771202@qq.com
 *   Author:         KennethAsher
 *   Description:    结果返回直接将112行注释的内容返回即可
 */

import com.bangying.utils.Constant;
import com.bangying.utils.PatternUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LawyerJudge {
    private final static Logger logger = LoggerFactory.getLogger(Doc.CaseParty.class);
    public static Pattern lawyer_spc_word_clear_pat = Pattern.compile("(.*代理[人]?)|(.*辩护[人]?)|([\\(（].*?[\\)）])|(律师)$|^(代理)");
    public static Pattern lawyer_accuser_pat = Pattern.compile(".*委托诉?讼?(代理人|辩护人|代理|辩护)[人]?([\\(（].*[\\)）])?[:：]?(.*律师(事务所)?)");
    public static Pattern doc_split_pat = Pattern.compile("。");
    public static Pattern lawyer_filter_pat = Pattern.compile("(.*(事务所|执业证号|援助|法律|中心|公司|一般代理|代理权限|一般授权|特别授权|特别代理|工作单位|专职律师|委托|代理|辩护|上诉|代表|第三人).*)|(^上[述列].*)|(^[该系].*)");
    public static Pattern name_general_clear_pat = Pattern.compile("(&.{1,7};)|([\\(（].*?[\\)）])|(<.*?>)|(二[ｏﾷ◎ㅇоΟО0０Ｏ〇◯○oO零].*)|�|\\+|-|\\?|？|([a-zA-Z])|\\\"|\\'|`|、|：|∶|:|;|；|=|［|］|（|）|/|_|／|<|﹤|>|﹥|&|＊|\\*|\\s|\\d|#|○|Ｘ|×|某|丨|被告|原告|统一社会信用代码");
    public static Pattern justice_patterns = Pattern.compile("^((代理)?审判长)[:：]?(.*)|^((保持队形)?院长)[:：]?(.*)|^((代|代理|助[理]?|人民)?审[判理]?员)[:：]?(.*)|^((代|代理|见习|实习)?书记员)[:：]?(.*)|^((保持队形)?法官助理)[:：]?(.*)|^((保持队形)?执行员)[:：]?(.*)|^((人[民员]|代理)?陪[审判]员?)[:：]?(.*)");
    public static Pattern jutices_pat = Pattern.compile("(院长|审判长|审[判理]员|代审[判理]员|代理审[判理]员|助理审[判理]员|人民审[判理]员|助审员|法官助理|执行员|书记员|人[民员]陪[审判]员?|陪[审判]员?|代理陪[审判]员?)[:：]?");
    public static Pattern opponents_pat = Pattern.compile("被告[人]?|被上诉|被|原审被告|被申请[人]?|被执行[人]?|被异议[人]?|罪犯");
    public static Pattern clear_date_pat = Pattern.compile("(申请执行.*?年)|(逾期不予执行)|((本|此).*?(与|和)原.*?核对无异)|(无异)|([ｏﾷ◎ㅇоΟО0０Ｏ〇◯○oO零一二三四五六七八九十×xX\\d]{1,4}年.*)|((一九)|(二[ｏﾷ◎ㅇоΟО0０Ｏ〇◯○oO零]).{1,4}年.*)|([一二三四五六七八九十xX\\d]{1,2}月[廿一二三四五六七八九十xX\\d]{1,3}日)|0霞住新乐市新小区西单元|文书日期");
    public static Pattern html_remove_pat = Pattern.compile(">(.*?)<");
    public static Pattern organ_split_pat = Pattern.compile("系|均系|为");

    public LawyerJudge() {}

    public static JavaRDD<String> cleanLawyerJudge(JavaRDD<String> rdd) throws IOException {
        logger.info("正式开始清洗");
        JavaRDD<String> lines_rdd = rdd.map(new Function<String, String>() {
            @Override
            public String call(String line_mainbody) throws Exception {
                //  因为提取的时候将换行替换成了了'。'，所以在文书可能连续出现多个句号，此处需要将这些替换掉
                String line = PatternUtils.sub(line_mainbody, html_remove_pat, "");
                String line_judge = line.replaceAll("。。。。。","").replaceAll("。。。。","").replaceAll("。。。","").replaceAll("。。","").replaceAll("\n","");
                String judges = "";
                String type_judge = "";
                String doc_num = line.split("\\|")[0];
                String[] lines = doc_split_pat.split(line_judge);
                String[] lines_judge = doc_split_pat.split(line_judge);
                //  清洗审判人员,遍历最后三行，原本遍历一行，但是会出现附文本（有审判员、书记员著名）会干扰到结果导致最终没有审判员
                int i = 0;
                for (int k = lines_judge.length-1; k >= 0; k--) {
                    if (jutices_pat.matcher(line).find()) {
                        line = PatternUtils.sub(line, Pattern.compile("\\s|\\\\|([\\(（].*?[\\)）])|(\\{.*?\\})|([\\[【].*?[\\]】])|\\?|？|　"), "");
                        line = PatternUtils.sub(line, Pattern.compile("&middot;"), "·");
                        if (line.contains("独任") || line.contains("批示")) {
                            continue;
                        }
                        String[] judge_list = jutices_pat.split(line);
                        int index = 2;
                        String judge_types = StringUtils.join(PatternUtils.findAll(line, jutices_pat), ",");
                        //  前面判断过匹配 审判人员，所以结果最少三个，如果存在index 1，就肯定会有2，有3肯定会有4以此类推
                        while (judge_list.length > index) {
                            String judge_name = judge_list[index];
                            //  为了避免很多没必要的正则匹配。所以名字如果大于5才进行正则判断，因为最不讲究的也得写个 “张三一月一日” 吧。。。。
                            if (judge_name.length()>5) {
                                judge_name = PatternUtils.sub(judge_name, clear_date_pat, "");
                            }
                            judge_name = PatternUtils.sub(judge_name, name_general_clear_pat, "");
                            //  处理完如果名字时是空的就丢掉吧
                            if (judge_name.length() > 0) {
                                //  有几十个sb写成这种格式“人民人民审判员  罗安树”名字长度大于3防止真有人叫x人民的 14f47031-9390-47a3-a512-a9ac00f79c98
                                if (judge_name.length() > 3) {
                                    if (judge_name.substring(3,4) == "附") {judge_name = judge_name.split("附")[0];}
                                    judge_name = PatternUtils.sub(judge_name, Pattern.compile("(人民|执行|见习|无误)$"), judge_name);
                                }
                                if (judge_name.length() > 2) {
                                    if (judge_name.substring(2,3) == "附") {judge_name = judge_name.split("附")[0];}}
                            }
                            if (judge_name.length() < 4) {judges += "," + judge_name ;}
                            index += 2;
                        }
                        if (judge_types.length() < 2) {continue;}
//                        System.out.println(judges.length());
                        try {
                            if (judges.length()<2){continue; }
                            type_judge = getJudges(judge_types, judges.substring(1));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (type_judge.length() > 10) {break;}
                        if (i == 3) {break;}
                        i = i+1;
                    }
                }
                //  清洗律师
                List<String> name_list = new ArrayList<String>();
                String friends = "";
                String oppnent = "";

                int flag = 0;
                for (String s: lines) {
                    if (s.contains("一案")) {
                        if (name_list.size() == 0) {
                            if (type_judge.contains("-")){
                                return doc_num+"|||"+type_judge+"||";}
//                                return "-";}
                            else {return "-";}
                        }
                        break;
                    }
                    s = PatternUtils.sub(s, Pattern.compile("\\s|\\\\"), "");
                    if (opponents_pat.matcher(s).find()) { flag = 1; }
//                    List<String> matcher_result = PatternUtils.findAll(s, lawyer_accuser_pat);
                    Matcher matcher_result = lawyer_accuser_pat.matcher(s);
                    if (matcher_result.find()) {
                        String lawyer_str =  PatternUtils.sub(matcher_result.group(0), Pattern.compile("[\\(（].*?[\\)）]"), "");
                        String[] lawyers = Pattern.compile("\\,|，|、|;|；|：|：|:").split(lawyer_str);
                        if (lawyers.length == 0) {break;}
                        lawyers[lawyers.length-1] = PatternUtils.sub(lawyers[lawyers.length-1], Pattern.compile("执行$|(律师)?专职$|律师$|(.*(援助|服务|中心|法律工作者|实习|助理|执业|兼职|职业|指派|指定|某).*)|(.*(X|M|H|Ｘ|×|x|m).*)|省|市"), "");
                        lawyers[lawyers.length-1] = PatternUtils.sub(lawyers[lawyers.length-1], Pattern.compile("律师事务$|律师律事务$"), "律师事务所");
                        if (!lawyers[lawyers.length-1].contains("律师事务所") || lawyers[lawyers.length-1].length()<3 || lawyers[0].contains("事务")) {break;}
                        if (lawyers.length > 1) {
                            int lawyers_length = lawyers.length;
                            for (int k = lawyers.length-2; k>=0; k--) {
                                if (lawyers[k] == "男" || lawyers[k] == "女"){ continue; }
                                if (Pattern.compile(".*[1-9].*").matcher(lawyers[k]).find() || Pattern.compile(".*年.*月.*日.*").matcher(lawyers[k]).find()) { continue; }
                                String lawyer_name = "";
                                try {
                                    lawyer_name = lawyers[k].split("人")[lawyers[k].split("人").length - 1];
                                } catch (Exception e) {
                                    lawyer_name = lawyers[k];
                                }
                                //  有的文书总存在将律师的名字写为地区名，有的以群众、文化程度来命名，有的使用甲，乙来命名
                                if ((lawyer_name.endsWith("省") || (lawyer_name.contains("省") && lawyer_name.length()>3)) ||
                                        (lawyer_name.endsWith("市") || (lawyer_name.contains("市") && lawyer_name.length()>3)) ||
                                        (lawyer_name.endsWith("县") || (lawyer_name.contains("县") && lawyer_name.length()>3)) ||
                                        (lawyer_name.endsWith("区") || (lawyer_name.contains("区") && lawyer_name.length()>3)) ||
                                        lawyer_name.contains("身份") || lawyer_name.endsWith("族") || lawyer_name.endsWith("群众") ||
                                        lawyer_name.endsWith("文化") || lawyer_name.startsWith("男") || lawyer_name.startsWith("女") ||
                                        lawyer_name.contains("联系") || lawyer_name.contains("电话") || lawyer_name.startsWith("住") ||
                                        lawyer_name.contains("个体") || lawyer_name.contains("大酒店") || lawyer_name.contains("上诉") ||
                                        lawyer_name.startsWith("曾用")) { continue; }
                                if (lawyer_name.length() > 3){
                                    if (lawyer_filter_pat.matcher(lawyer_name).find()) { lawyer_name = PatternUtils.sub(lawyer_name, lawyer_filter_pat, ""); }
                                }
                                lawyer_name = PatternUtils.sub(lawyer_name, name_general_clear_pat, "");
                                if (lawyers[lawyers_length-1].endsWith("分所")) {lawyers[lawyers_length-1] = lawyers[lawyers_length-1].split("律师事务所")[0]+"律师事务所";}
                                if (lawyers[lawyers_length-1].startsWith("系") || lawyers[lawyers_length-1].startsWith("均系") || lawyers[lawyers_length-1].startsWith("为")){
                                    lawyers[lawyers_length-1] = organ_split_pat.split(lawyers[lawyers_length-1])[organ_split_pat.split(lawyers[lawyers_length-1]).length-1];
                                }
                                if (lawyer_name.length() > 1 && lawyers_length>5) {
                                    if (name_list.contains(lawyer_name)) { continue; }
                                    if (flag == 0) { friends = friends+","+lawyer_name+"-"+lawyers[lawyers_length-1]; }
                                    if (flag == 1) { oppnent = oppnent+","+lawyer_name+"-"+lawyers[lawyers_length-1]; }
                                    name_list.add(lawyer_name);
                                }
                            }
                        }
                    }
                }

                String[] out_friends = new String[0];
                String[] out_oppnents = new String[0];
                String out_line = "-";
                String[] temp_friends = new String[0];
                String[] temp_oppnents = new String[0];
                if (friends.length() > 0) { out_friends = friends.substring(1).split(","); }
                if (oppnent.length() > 0) { out_oppnents = oppnent.substring(1).split(","); }
                if (out_friends.length > 0) {
                    for(int fs=0; fs<out_friends.length; fs++) {
                        temp_friends = Constant.delete(fs, out_friends);
                        if (out_line.startsWith("-")) {out_line = out_line.substring(1);}
                        out_line += doc_num + "|" + out_friends[fs].split("-")[0] +"|"+ out_friends[fs].split("-")[out_friends[fs].split("-").length-1]
                                +"|"+ type_judge +"|"+ StringUtils.join(temp_friends,",") +"|"+ StringUtils.join(out_oppnents,",")+"@!";
                    }
                }
                if (out_oppnents.length > 0) {
                    for(int os=0; os<out_oppnents.length; os++) {
                        temp_oppnents = Constant.delete(os, out_oppnents);
                        if (out_line.startsWith("-")) {out_line = out_line.substring(1);}
                        out_line += doc_num + "|" + out_oppnents[os].split("-")[0] +"|"+ out_oppnents[os].split("-")[out_oppnents[os].split("-").length-1]
                                +"|"+ type_judge +"|"+ StringUtils.join(temp_oppnents,",") +"|"+ StringUtils.join(temp_friends,",")+"@!";
                    }
                }
                return out_line;
            }
        });
        JavaRDD<String> result_rdd = lines_rdd.flatMap(line -> Arrays.asList(line.split("@!")).iterator());
        JavaRDD<String> out_rdd = result_rdd.filter(line -> !line.equals("-"));
        return out_rdd;
    }

    public static String getJudges(String judge_types, String judges) {
        String[] fields_type = judge_types.split(",");
        String[] fields_judge = judges.split(",");
        String type_judge = "";
        int flag = (fields_judge.length < fields_type.length) ? fields_judge.length : fields_type.length;
        for (int i = 0; i < flag; i++) {
            if(fields_judge[i].length()<2) {
                continue;
            }
            type_judge = type_judge + ',' + fields_type[i] + '-' + fields_judge[i];
        }
        if (type_judge.length()<2){return "";}
        return type_judge.substring(1);
    }


}
