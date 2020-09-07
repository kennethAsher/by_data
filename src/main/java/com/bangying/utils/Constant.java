package com.bangying.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName:      Constant
 * Package:        cleandoc.utils
 * Datetime:       2020/7/27   3:51 下午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    放置一些琐杂的方法
 */



public class Constant {
    private static Properties properties;
    private final static Logger logger = LoggerFactory.getLogger(Constant.class);
    static {
        try {
            properties = new Properties();
            properties.load(Constant.class.getResourceAsStream("/conf.properties"));
        } catch (Exception e) {
            logger.error("初始化配置文件失败:"+e.getMessage());
        }
    }
    public Constant() throws IOException{}

    public static String getProperty(String key) throws IOException {
        return properties.getProperty(key);
    }
    public static String[] delete(int index, String array[]) {
        //数组的删除其实就是覆盖前一位
        String[] arrNew = new String[array.length - 1];
        for (int i = 0; i < array.length - 1; i++) {
            if (i < index) {
                arrNew[i] = array[i];
            } else {
                arrNew[i] = array[i + 1];
            }
        }
        return arrNew;
    }
}
