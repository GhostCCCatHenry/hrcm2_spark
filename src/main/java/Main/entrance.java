package Main;

import Service.App;
import scala.tools.nsc.Global;

public class entrance {

    public static void main(String[] args) {

//        String str1 = "D:\\geneEXP\\HRCMspark\\chr1.fa";

//        String str1 = "/home/gene/hg13/chr1.fa";
        String str1 = args[0];//参考序列路径
        String str2 = args[1];//带压缩文件夹路径
        String str3 = "/home/gene/out";//中间结果写死
        String Out = args[2];//输出路径
//        String str2 = "hdfs://master:9000/chr1/1/";
//        String str1 ="D:\\geneEXP\\HRCMspark\\chr1.fa";
//        String str2 = "D:\\geneEXP\\{chr1/*}";
//        String str3 = "D:\\geneEXP\\HRCMspark\\out";//中间结果写死
//        String Out = "D:\\geneEXP\\";
        App.compress(str1,str2,str3);
        tar t = new tar();
        t.FSfetch(str3,Out);
        // libbsc-master/bsc e out.tar out.bsc
    }



}
