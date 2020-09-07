package com.bangying.main.clean;

/*
 * ClassName:       SupplementAddData
 * Package:         main
 * Datetime:        2020/7/27   3:39 下午
 * E-Mail:          1131771202@qq.com
 * Author:          KennethAsher
 * Description:     对原本提取的organ_add_data数据进行补充和分类
 */

import com.bangying.cleandata.SupplementAddData;
import com.bangying.utils.Constant;
import com.bangying.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SupplementAddDataHandler {
    private final static Logger logger = LoggerFactory.getLogger(SupplementAddDataHandler.class);

    public static void main(String[] args) throws IOException {
        logger.info("##########开始执行任务：补充organ_add_data");
        //    获取organ_add_data路径下的所有文件名称
//        FileSystem file_system = HdfsUtils.getFileSystem()
        JavaSparkContext spark_context = SparkUtils.getSparkContext(Constant.getProperty("supplement_add_data_appname"));
        JavaRDD<String> text_rdd = spark_context.textFile(Constant.getProperty("organ_add_data_path"));
        JavaRDD<String> updated_rdd = SupplementAddData.updateData(text_rdd);
        logger.info("##########数据补充完成，开始写出：");
        updated_rdd.repartition(100);
        updated_rdd.saveAsTextFile(Constant.getProperty("add_data_path"));
        SparkUtils.close(spark_context);
    }


}
