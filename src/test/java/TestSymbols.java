import java.util.HashMap;
import java.util.Map;

/**
 * ClassName:      TestSymbols
 * Package:        PACKAGE_NAME
 * Datetime:       2020/7/28   4:55 下午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:     测试特殊符号的展示
 */
public class TestSymbols {
    public static void main(String[] args) {
        String line = "";
        String[] fields = line.split("\\|");
        System.out.println(fields.length);
        Map<String, String> map = new HashMap<String, String>();
        map.put("name","zhangsan");
        System.out.println(map);
    }
}
