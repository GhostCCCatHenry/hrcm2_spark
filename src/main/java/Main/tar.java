package Main;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Objects;

public class tar {
    //    private FSDataInputStream fsDataInputStream = null;
    public void FSfetch(String outfile,String finalOut){
        Configuration conf = new Configuration();
        Path inpath = new Path(outfile);
//        Path tmp = new Path("D:\\geneEXP\\out");//中间结果
        String tmp = "/home/gene";
        //这里指定使用的是HDFS文件系统
        //通过如下的方式进行客户端身份的设置
//        System.setProperty("HADOOP_USER_NAME","root");
        //也可以通过如下的方式去指定文件系统的类型，并且同时设置用户身份
//        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"),conf,"root");
        try {
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf);
            File dir = new File(tmp+"/out");
            File output = new File(finalOut);
            System.out.println(deleteFile(dir));
            fs.moveToLocalFile(inpath,new Path(tmp));
//            File dir = new File("D:\\geneEXP\\out\\");
            archive(dir,new File(tmp+"/out.tar"));
            if(!output.exists()) System.out.println(output.mkdir());
            callShell("./bsc e "+tmp+"/out.tar "+finalOut+"/out.bsc");
            System.out.println(deleteFile(dir));
            System.out.println(deleteFile(new File(tmp+"/out.tar")));
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //删除文件夹
    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }
        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {
            for (File file : Objects.requireNonNull(dirFile.listFiles())) {
                deleteFile(file);
            }
        }
        return dirFile.delete();
    }

    public static void archive(File srcFile, File destFile) throws Exception {
        TarArchiveOutputStream taos = new TarArchiveOutputStream(
                new FileOutputStream(destFile));
        archive(srcFile, taos, "");
        taos.flush();
        taos.close();
    }

    private static void archive(File srcFile, TarArchiveOutputStream taos,
                                String basePath) throws Exception {
        if (srcFile.isDirectory()) {
            archiveDir(srcFile, taos, basePath);
        } else {
            archiveFile(srcFile, taos, basePath);
        }
    }

    private static void archiveDir(File dir, TarArchiveOutputStream taos,
                                   String basePath) throws Exception {
        File[] files = dir.listFiles();
        if (files.length < 1) {
            TarArchiveEntry entry = new TarArchiveEntry(basePath
                    + dir.getName() + File.separator);

            taos.putArchiveEntry(entry);
            taos.closeArchiveEntry();
        }
        for (File file : files) {
            // 递归归档
            archive(file, taos, basePath + dir.getName() + File.separator);
        }
    }

    /**
     * 数据归档
     */
    private static void archiveFile(File file, TarArchiveOutputStream taos,
                                    String dir) throws Exception {
        /**
         * 归档内文件名定义
         *
         * <pre>
         * 如果有多级目录，那么这里就需要给出包含目录的文件名
         * 如果用WinRAR打开归档包，中文名将显示为乱码
         * </pre>
         */
        TarArchiveEntry entry = new TarArchiveEntry(dir + file.getName());

        entry.setSize(file.length());

        taos.putArchiveEntry(entry);

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(
                file));
        int count;
        byte []data = new byte[1<<12];//buffer 容量
        while ((count = bis.read(data, 0, 1<<12)) != -1) {
            taos.write(data, 0, count);
        }

        bis.close();

        taos.closeArchiveEntry();
    }

    public static void callShell(String shellString) {
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                System.out.println("call shell failed. error code is :" + exitValue);
            }
        } catch (Throwable e) {
            System.out.println("call shell failed. " + e);
        }
    }
}
