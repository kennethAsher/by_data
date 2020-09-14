package com.bangying.main.clean;

import com.bangying.cleandata.Doc;
import com.bangying.cleandata.PublicExpense;
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
 * Datetime:       2020/7/30   10:29
 * E-Mail:         1131771202@qq.com
 * Author:         KennethAsher
 * Description:    此方法是清洗裁判文书的受理费用一系列内容
 */

public class PublicExpenseHandler {
    private final static Logger logger = LoggerFactory.getLogger(CasePartyHandler.class);
    public static void main(String[] args) throws IOException {
        logger.info("开始执行清洗官费任务");
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("public_expense_appname"));
        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_data_path"));
        logger.info("开始清洗任务");
        JavaRDD<String> rdd_result = PublicExpense.cleanPublicExpense(text_rdd);
        rdd_result.repartition(100);
        rdd_result.saveAsTextFile(Constant.getProperty("public_expense_path"));
    }
}
