package com.bangying.utils;
/*
 *   ClassName:      PatternUtils
 *   Package:        com.bangying.utils
 *   Datetime:       2020/8/3   3:02 下午
 *   E-Mail:         1131771202@qq.com
 *   Author:         KennethAsher
 *   Description:    正则表达是方法
 */

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternUtils {
    public PatternUtils(){}
    // 返回所有匹配到的内容
    public static List<String> findAll(String line, Pattern pattern) {
        Matcher matcher = pattern.matcher(line);
        List<String> all_list = new ArrayList<String>();
        while(matcher.find()) {
            all_list.add(matcher.group());
        }
        return all_list;
    }
    // 替换匹配到字符串指定内容
    public static String sub(String line, Pattern pattern, String field) {
        Matcher matcher = pattern.matcher(line);
        return matcher.replaceAll(field);
    }
}
