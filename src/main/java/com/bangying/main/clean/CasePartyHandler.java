package com.bangying.main.clean;

import com.bangying.cleandata.Doc;
import com.bangying.utils.Constant;
import com.bangying.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ClassName:      CasePartyHandler
 * Package:        com.bangying.main
 * Datetime:       2020/7/30   10:29 上午
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    此方法是清洗裁判文书，从之中找到原告，被告以及案由等。（包括诉求方身份）
 */
public class CasePartyHandler {
    private final static Logger logger = LoggerFactory.getLogger(CasePartyHandler.class);

    public static void main(String[] args) throws IOException {
        logger.info("开始执行清洗裁判文书，寻找内容原告，被告，案由");
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("Case_party_appname"));
        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_data_path"));
        logger.info("开始清洗。。。");
        JavaRDD<String> rdd_result = Doc.CaseParty.cleanCaseParty(text_rdd);
        rdd_result.repartition(5);
        rdd_result.saveAsTextFile(Constant.getProperty("case_party_path"));
        spark_context.stop();
    }
}
