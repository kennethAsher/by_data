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

//Logger logger = LoggerFactory.getLogger(TestPattern.class.getName());
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

//        String line = "勾引门卫大爷一案中，勾引日本鬼子";
//        String pat = "|勾引|日本";
//        Pattern pattern1 = Pattern.compile("勾引");
//        System.out.println(findAll(line, pattern1).get(2));
//        Pattern pattern = Pattern.compile("%s".format(pat.substring(1)));
//        Pattern clean_pattern = Pattern.compile("%s".format(pat));
//        List<String> all_list = findAll(line, clean_pattern);
//        String[] list = pattern.split(line);
//        System.out.println(list[1]);
//        System.out.println(pattern);

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class TestPattern {
    public static void main(String[] args) {
        String line = "This order was placed for QT3000! OK?";
        String pattern = "(\\D*)(\\d+)(.*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);
        if (m.find()) {
            System.out.println("Found value: " + m.group(0));
            System.out.println("Found value: " + m.group(1) );
            System.out.println("Found value: " + m.group(2) );
            System.out.println("Found value: " + m.group(3) );

        }
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
