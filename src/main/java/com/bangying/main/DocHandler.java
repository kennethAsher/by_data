package com.bangying.main;

import com.bangying.cleandata.Doc;
import com.bangying.utils.Constant;
import com.bangying.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * ClassName:      CasePartyHandler
 * Package:        com.bangying.main
 * Datetime:       2020/7/30   10:29 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    清洗裁判文书-启动，将所有需要清洗的内容放在此处统一开始执行，最后合并的一键启动程序
 */
public class DocHandler {
    private final static Logger logger = LoggerFactory.getLogger(DocHandler.class);

    public static void main(String[] args) throws IOException {
        logger.info("开始执行清洗裁判文书");
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("supplement_add_data_appname"));
        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_add_data_path"));
        Map<String, JavaRDD<String>> rdd_map = Doc.cleanDoc(text_rdd);
    }
}
