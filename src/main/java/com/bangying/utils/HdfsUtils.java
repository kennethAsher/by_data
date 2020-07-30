package com.bangying.utils;

/**
 * @BelongsProject: by_data
 * @BelongsPackage: cleandoc.utils
 * @Author: kennethAsher
 * @CreateTime: 2020-03-26 19:17
 * @Email 1131771202@qq.com
 * @Description: 与hdfs上的文件交互的工具类
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem;
    }

    //拿到当前路径下的所有文件名
    public static List<String> catFileName(Path path, FileSystem file_system) throws IOException {
        List<String> file_name_list = new ArrayList<String>();
        for (FileStatus fs : file_system.listStatus(path)) {
            file_name_list.add(fs.getPath().getName());
        }
        return file_name_list;
    }
}
