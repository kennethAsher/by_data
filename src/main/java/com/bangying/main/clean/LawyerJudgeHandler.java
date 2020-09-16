package com.bangying.main.clean;
/*
 *   ClassName:      LawyerJudgeHandler
 *   Package:        com.bangying.main
 *   Datetime:       2020/8/17   2:38 下午
 *   E-Mail:         1131771202@qq.com
 *   Author:         KennethAsher
 *   Description:
 */

import com.bangying.cleandata.LawyerJudge;
import com.bangying.utils.Constant;
import com.bangying.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 在这里和python处理的不相同，是将没有清洗出内容的数据全部社区，而不是保留空数值
 * 全部执行回导致执行速度过慢，所以需要间接性的执行，每个文件单独执行
 *
 */
public class LawyerJudgeHandler {
    private final static Logger logger = LoggerFactory.getLogger(LawyerJudgeHandler.class);

    public static void main(String[] args) throws IOException {
        logger.info("开始执行清洗裁判文书，寻找律师审判人员等详细信息");
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("lawyer_judge_appname"));
        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_data_path"));
        logger.info("开始清洗");
        JavaRDD<String> result_rdd = LawyerJudge.cleanLawyerJudge(text_rdd);
        result_rdd.repartition(100);
        result_rdd.saveAsTextFile(Constant.getProperty("lawyer_judge_path"));
        spark_context.stop();
    }
}
