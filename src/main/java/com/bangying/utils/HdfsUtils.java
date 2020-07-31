package com.bangying.utils;

/*
 * @BelongsProject: by_data
 * @BelongsPackage: cleandoc.utils
 * @Author: kennethAsher
 * @CreateTime: 2020-03-26 19:17
 * @Email 1131771202@qq.com
 * @Description: 与hdfs上的文件交互的工具类
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

public class HdfsUtils {

    public HdfsUtils() throws IOException {
    }

    //拿到配置好的fileSystem
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
    }

    //拿到当前路径下的所有文件名
    public static List<String> catFileName(Path path, FileSystem file_system) throws IOException {
        List<String> file_name_list = new ArrayList<String>();
        for (FileStatus fs : file_system.listStatus(path)) {
            file_name_list.add(fs.getPath().getName());
        }
        return file_name_list;
    }

    //在hdfs中拿到cause各个级别的案由
    public static Map<String,Set<String>> getCauseSet() throws IOException {
        Set<String> set_one = new HashSet<String>();
        Set<String> set_two = new HashSet<String>();
        Set<String> set_three = new HashSet<String>();
        Set<String> set_four = new HashSet<String>();
        Map<String,Set<String>> set_map = new HashMap<String, Set<String>>();
        Path path = new Path(Constant.getProperty("cause_of_action_path"));
        FileSystem fs = HdfsUtils.getFileSystem();
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\\|");
            if (fields.length<9){continue;}
            if (!fields[5].equals("")) {set_four.add(fields[5]);}
            if (!fields[4].equals("")) {set_three.add(fields[4]);}
            if (!fields[3].equals("")) {set_two.add(fields[3]);}
            if (!fields[2].equals("")) {set_one.add(fields[2]);}
        }
        set_map.put("set_one", set_one);
        set_map.put("set_two", set_two);
        set_map.put("set_three",set_three);
        set_map.put("set_four",set_four);
        reader.close();
        return set_map;
    }

}
