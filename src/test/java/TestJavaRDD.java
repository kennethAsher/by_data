import com.bangying.main.clean.CasePartyHandler;
import com.bangying.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestJavaRDD {
    private final static Logger logger = LoggerFactory.getLogger(CasePartyHandler.class);
    public static void main(String[] args) throws IOException {
        logger.info("开始执行清洗裁判文书，寻找律师审判人员等详细信息");
        JavaSparkContext spark_context = SparkUtils.getSparkContext("test_appname");
        JavaRDD<String> text_rdd = spark_context.textFile("b.txt");
        logger.info("开始清洗。。。");
        List<String> list = new ArrayList<String>();
//        list.add("zhangsan");
//        list.add("lisi");
//        JavaRDD<String> rdd = spark_context.parallelize(list);
        JavaRDD<String> out_rdd = text_rdd.map(new Function<String, String>() {
            @Override
            public String call(String line_mainbody) throws Exception {
                List<String> out_list = new ArrayList<String>();
                String lines = "";
                for (int i = 0; i<10;i++){
                    lines += line_mainbody+" ";
                }
                return lines;
            }
        });
//        text_rdd.repartition(1);
        JavaRDD<String> result_rdd = out_rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        result_rdd.saveAsTextFile("out");

    }
}
