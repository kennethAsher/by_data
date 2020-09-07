package com.bangying.main.clean;

/**
 * @BelongsProject: by_data
 * @BelongsPackage: main
 * @Author: kennethAsher
 * @CreateTime: 2020-03-26 14:31
 * @Email 1131771202@qq.com
 * @Description: 第一步执行的文件，这个文件负责清洗出法院，文书种类，案号
 */

import com.bangying.utils.Constant;
import com.bangying.utils.HdfsUtils;
import com.bangying.utils.SparkUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class WordCount {
    //    主函数
    public static void main(String[] args) throws IOException {
//        String age = "zhangsan";
//        Path path_os = new Path("hdfs:///test/spark/input");
        String appName = "JavaSparkDemo";
//        FileSystem fileSystem = HdfsUtils.getFileSystem();
//        List<String> nameList = HdfsUtils.catFileName(path_os, fileSystem);
//        JavaSparkContext sc = null;
//        //初始化 JavaSparkContext
//        SparkConf conf = new SparkConf().setAppName(appName);
//        sc = new JavaSparkContext(conf);
        JavaSparkContext java_spark_context = SparkUtils.getSparkContext(appName);
        JavaRDD<String> lines = java_spark_context.textFile(Constant.getProperty("input_file"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] arr = s.split(" ");
                for(String ss: arr){
                    list.add(ss);
                }
                return list.iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        wordCounts.saveAsTextFile(Constant.getProperty("output_path"));
        SparkUtils.close(java_spark_context);
    }

}
