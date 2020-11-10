package edu.ucr.cs.cs226.jge013;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class HDFSUpload {
    private Configuration conf;
    private FileSystem local_fs;
    private FileSystem hdfs_fs;

    public HDFSUpload() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        local_fs = FileSystem.getLocal(conf);
        hdfs_fs = FileSystem.get(conf);
    }
    public void copy(String srcPath, String dstPath){
        FileSystem srcFileSystem = getFileSystem(srcPath);
        FileSystem dstFileSystem = getFileSystem(dstPath);
        Path src_path = new Path(srcPath);
        Path dst_path = new Path(dstPath);

        try {
            checkExists(srcFileSystem,dstFileSystem,src_path,dst_path);
            FSDataInputStream inputStream = srcFileSystem.open(src_path);
            FSDataOutputStream outputStream = dstFileSystem.create(dst_path);
            System.out.println("Copy file from Source FS: " + srcPath + " To Target FS: " + dstPath + "\n");
            long startTime = System.currentTimeMillis();
            IOUtils.copyBytes(inputStream, outputStream,4096, true);
            long endTime = System.currentTimeMillis();
            System.out.println("The overhead of Time:" + (endTime - startTime) + " ms\n");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    public FileSystem getFileSystem(String path){
        if(path.indexOf("hdfs") >= 0) {
            return hdfs_fs;
        }else{
            return local_fs;
        }

    }
    public void checkExists(FileSystem srcFileSystem, FileSystem dstFileSystem, Path srcPath, Path dstPath) throws Exception {
        if(!srcFileSystem.exists(srcPath)){
            throw new Exception("This file can not be uploaded since it does not exist in Source File System\n");
        }
        if(dstFileSystem.exists(dstPath)){
            throw new Exception("This file already exists in Target File system\n");
        }

    }
    public static void main(String[] args) {
        String srcPathName, dstPathName;
        if(args.length==2){
            srcPathName = args[0];
            dstPathName = args[1];
        } else {
            System.err.println("Two arguments are required: srcPathName and dstPathName\n");
            return;
        }


        try {
            HDFSUpload fileService = new HDFSUpload();
            fileService.copy(srcPathName, dstPathName);
        } catch (Exception e){
            e.printStackTrace();
        }
    }


}
