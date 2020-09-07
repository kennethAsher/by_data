import com.bangying.utils.Constant;

import java.util.*;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * ClassName:      TestString
 * Package:        PACKAGE_NAME
 * Datetime:       2020/7/30   10:37 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    测试字符串功能
 */


/**
 //        String line = "I am LiLei, do you have friend?";
 //        StringBuilder line = new StringBuilder("xiaoming");
 //        System.out.println(line.substring(0,line.indexOf("do")));
 //        System.out.println(String.format(line, "k哥"));

 //        String[] lines = "zhangsan lisi wangmazi liuwu".split(" ");
 //        for (int i = lines.length-1; i >= 0; i--) {
 //            System.out.println(lines[i]);
 //        }
 //        line.append("|").append("zhangsan");
 //        System.out.println(line.substring(0));


 //        String ll = "xiaomgidhfsdgndgmadgmasdfms";
 //        ll = ll.replaceAll("m", "");
 //        System.out.println(ll);

 //        Set<String> set = new HashSet<String>();
 //        set.add("zhangsan");
 //        set.add("lisi");
 //        for (String field : set) {
 //            System.out.println(field);
 //        }
 //        for (String field : set) {
 //            System.out.println(field);
 //        }
 */

public class TestString {
    public static void main(String[] args) {
//        String line = "zhangsan lisi wangmazi";
//        String[] fields = line.split(" ");
//        String[] out = new String[0];
//        out = Constant.delete(0, fields);
//        for (String field : fields){
//            System.out.println(field);
//        }
//        for (String o: out){
//            System.out.println(o);
//        }
//        String join = join(out, "-");
//        System.out.println(join);
//        System.out.println(out.length);
        String name = "asd";
        String[] name_list = name.split(",");
        System.out.println(name.split(",")[name.split(",").length - 1]);


    }
}
