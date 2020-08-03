/**
 * ClassName:      TestPattern
 * Package:        PACKAGE_NAME
 * Datetime:       2020/7/28   1:42 下午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    测试拿到正则中的最后一位
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class TestPattern {
    public static void main(String[] args) {
//        Logger logger = LoggerFactory.getLogger(TestPattern.class.getName());
//        Pattern pattern = Pattern.compile("[,，]");
//        String line = "张三，里斯，wangmzi,peterchen,xxx";
//        String[] court = pattern.split(line);
//        for (String c : court) {
//            System.out.println(c);
//        }

//        Pattern clean_pattern = Pattern.compile("[,，]");
//        Matcher matcher = clean_pattern.matcher(line);
//        String clean_line = matcher.replaceAll("");
//        System.out.println(clean_line);

        String line = "aaacabaxacasac";
        Pattern clean_pattern = Pattern.compile("ac");
        List<String> all_list = findAll(line, clean_pattern);
        System.out.println(all_list);
    }

    public static List<String> findAll(String line, Pattern pattern) {
        Matcher matcher = pattern.matcher(line);
        List<String> all_list = new ArrayList<String>();
        while(matcher.find()) {
            all_list.add(matcher.group());
        }
        return all_list;
    }
}
