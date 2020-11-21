package Main;

import Service.App;
import scala.tools.nsc.Global;

import java.io.IOException;

public class entrance {

    public static void main(String[] args) throws IOException {

        String str1 = args[0];//参考序列路径
        String str2 = args[1];//带压缩文件夹路径
        String str3 = "/gene/out";//中间结果写死
        String Out = args[2];//输出路径
//        String str2 = "hdfs://master:9000/chr1/1/";
//        String str1 = "J:\\gene\\hg16_chr21.fa";
//        String str2 = "J:\\gene\\{test_chr21/*}";
//        String str3 = "J:\\gene\\out";//中间结果写死
//        String Out = "D:\\geneEXP\\";
        App.compress(str1,str2,str3);
        tar t = new tar();
        t.FSfetch(str2,str3,Out);
    }



}
