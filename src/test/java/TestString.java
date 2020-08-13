import java.util.HashSet;
import java.util.Set;

/**
 * ClassName:      TestString
 * Package:        PACKAGE_NAME
 * Datetime:       2020/7/30   10:37 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    测试字符串功能
 */
public class TestString {
    public static void main(String[] args) {
//        测试句子中添加标题
//        String line = "I am LiLei, do you have friend?";
//        StringBuilder line = new StringBuilder("xiaoming");
//        System.out.println(line.substring(0,line.indexOf("do")));
//        System.out.println(String.format(line, "k哥"));

//        String[] lines = "zhangsan lisi wangmazi liuwu".split(" ");
//        line.append("|").append("zhangsan");
//        System.out.println(line.substring(0));


//        String line = "zha|ngsan";
//        String[] fields = line.split("\\|");
//        System.out.println(fields[0]);

        Set<String> set = new HashSet<String>();
        set.add("zhangsan");
        set.add("lisi");
        for (String field : set) {
            System.out.println(field);
        }
        for (String field : set) {
            System.out.println(field);
        }

    }
}
