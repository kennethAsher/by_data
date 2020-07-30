package com.bangying.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * ClassName:      SparkUtils
 * Package:        cleandoc.utils
 * Datetime:       2020/7/27   5:40 下午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    常用的Spark工具类
 */
public class SparkUtils {

    public SparkUtils() {}
//  获取sparkContext
    public static JavaSparkContext getSparkContext(String appName) {
        //初始化 JavaSparkContext
        SparkConf conf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(conf);
    }
//  获取lineRdd
    public static JavaRDD<String> getLineRdd(String path, JavaSparkContext sparkContext) {
        return sparkContext.textFile(path);
    }

//  关闭spark
    public static void close(JavaSparkContext java_spark_context){
        java_spark_context.stop();
    }

}
