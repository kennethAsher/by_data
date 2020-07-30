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

import java.util.regex.Pattern;
public class TestPattern {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(TestPattern.class.getName());
        Pattern pattern = Pattern.compile("NULL|\\+|,|-|\\.|0|1|2|3|4|5|6|7|8|9|>|\\?|null|VF|_|`|·|ˎ̥|‘|、|【");
        String line = "NULL河北理工法院";
        String[] court = pattern.split(line);
        System.out.println(court[court.length - 1]);
//        logger.info("正在输出"+court);
    }
}
