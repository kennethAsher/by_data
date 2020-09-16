package com.bangying.main.clean;

import com.bangying.cleandata.Doc;
import com.bangying.cleandata.PublicExpense;
import com.bangying.utils.Constant;
import com.bangying.utils.HdfsUtils;
import com.bangying.utils.SparkUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.StringUtil;

import java.io.IOException;
import java.util.List;

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
        FileSystem fileSystem = HdfsUtils.getFileSystem();
        List<String> nameList = HdfsUtils.catFileName(new Path(Constant.getProperty("organ_data_path")), fileSystem);
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("public_expense_appname"));
//        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_data_path"));
        int flag = 0;
        for (String name: nameList) {
            flag += 1;
            JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_data_path")+name);
            logger.info("开始执行"+Constant.getProperty("organ_data_path")+name+",第"+flag+"个文件");
            JavaRDD<String> rdd_result = PublicExpense.cleanPublicExpense(text_rdd);
            rdd_result.repartition(5);
            rdd_result.saveAsTextFile(Constant.getProperty("public_expense_path")+name);
        }
        spark_context.stop();
    }
}
