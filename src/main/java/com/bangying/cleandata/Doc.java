package com.bangying.cleandata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName:      Doc
 * Package:        com.bangying.cleandata
 * Datetime:       2020/7/30   11:28 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    清洗裁判文书,将所有的裁判文书清洗统一放在这里执行,最后需要合并的
 */
public class Doc implements ForeachFunction<Row> {
    private final static Logger logger = LoggerFactory.getLogger(CaseParty.class);

    public Doc() {}
    //  返回清洗的内容doc
    public static Map<String,JavaRDD<String>> cleanDoc(JavaRDD<String> RDD) throws IOException {
        return run(RDD);
    }

    //  开始清洗doc的主要运行函数
    public static Map<String, JavaRDD<String>> run(JavaRDD<String> rdd) throws IOException{
        Map<String, JavaRDD<String>> rdd_map = new HashMap<String, JavaRDD<String>>();
        JavaRDD<String> lines_rdd = rdd.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {

                return null;
            }
        });

        JavaRDD<String> case_party_rdd = lines_rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s=="-" ? false : true;
            }
        });
        rdd_map.put("case_party_rdd", case_party_rdd);
        return rdd_map;
    }


    //    必须实现的方法，否则会报错
    @Override
    public void call(Row row) throws Exception {

    }
}
