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
        String line = "this is ok, %s is good";
        System.out.println(String.format(line, "k哥"));
    }
}
