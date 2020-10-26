package Service;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.storage.StorageLevel;
import pojo.*;
import scala.Tuple2;
import scala.Tuple2$;
import scala.Tuple3;

import java.io.*;
import java.util.*;

import static java.lang.Math.*;
import static org.apache.spark.api.java.StorageLevels.*;

public class App {

    private static final int VEC_SIZE = 1 << 20;
//    private static final int PERCENT = 15; //the percentage of compressed sequence number uses as reference
    private static final int kMerLen = 13; //the length of k-mer
    private static final int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static final int hashTableLen = 1 << kmer_bit_num; // length of hash table
//    private static int sec_seq_num = 120;
    private static int seqBucketLen = getNextPrime(1<<20);
    //kryo序列化注册类，自定义的类必须要经过注册
    private static void createRefBroadcast(reference_type ref,String filename){
        String str;
        char []cha;
        int _seq_code_len = 0, _ref_low_len = 1, letters_len = 0;//record lowercase from 1, diff_lowercase_loc[i]=0 means mismatching
        boolean flag = true;
        //int []ref_low_begin = new int[500]; int []ref_low_length = new int[500]; char[] ref_seq = new char[1000];
        char temp_cha;
        File file = new File(filename);

        //最好把参考序列读取本地化，集群化可能会拖慢速度！Driver与Executor的关系！
/*        Configuration conf = new Configuration();//访问hdfs用 Hadoop配置对象。
        Path path = new Path(filename);*/
        try {
/*            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream is = fs.open(path);//创造HDFS输入流*/
            BufferedReader br = new BufferedReader(new FileReader(file));//使用Reader缓冲输入流的信息，被BufferedReader读取
            str = br.readLine();
            while((str=br.readLine())!=null){
                cha = str.toCharArray();
                for (char a: cha) {
                    temp_cha=a;
                    if(Character.isLowerCase(temp_cha)){
                        if (flag) //previous is upper case
                        {
                            flag = false; //change status of flag
                            ref.set_Ref_low_begin_byturn(letters_len,_ref_low_len);
                            //ref_low_begin[_ref_low_len] = letters_len;
                            letters_len = 0;
                        }
                        temp_cha = Character.toUpperCase(temp_cha);
                    }
                    else {
                        if (!flag)  //previous is lower case
                        {
                            flag = true;
                            ref.set_Ref_low_length_byturn(letters_len,_ref_low_len);
                            _ref_low_len++;
                            //ref_low_length[_ref_low_len++] = letters_len;
                            letters_len = 0;
                        }
                    }
                    if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T'){
                        ref.set_Ref_code_byturn(temp_cha,_seq_code_len);
                        _seq_code_len++;
                    }
                    letters_len++;
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ref.set_Ref_code_len(_seq_code_len);
        ref.set_Ref_low_len(_ref_low_len);

        kMerHashingConstruct(ref);
    }

    private static compression_type createSeqRDD(Iterator<Tuple2<Text,Text>> s){
        //这一部分才由Executor执行，所以涉及的两个type必须要封装序列化！
        int letters_len = 0, n_letters_len = 0;
        boolean flag = true, n_flag = false;
        char temp_cha;

        String str;
        char[] cha;      //the content of one line
        compression_type c = new compression_type();
        c.identifier = s.next()._1.toString();
//        c.identifier = s.next();

        while (s.hasNext()){
            str = s.next()._1.toString();
            if(c.line==0)
                c.line = str.length();
            cha=str.toCharArray();
            //有不满行！
            for (int i = 0; i < cha.length; i++){
                temp_cha = cha[i];
                if (Character.isLowerCase(temp_cha)) {//previous is upper case
                    if (flag) {
                        flag = false;
                        c.addSeq_low_begin(letters_len);
                        letters_len = 0;
                    }
                    temp_cha = Character.toUpperCase(temp_cha);
                }
                else {
                    if (Character.isLowerCase(temp_cha)) {
                        if (!flag) {
                            flag = true;
                            c.addSeq_low_length(letters_len);
                            letters_len = 0;
                        }
                    }
                }
                letters_len++;

                //temp_cha is an upper letter
                if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T')
                    c.addSeq_code(temp_cha);
                else if (temp_cha != 'N') {
                    c.addSpe_cha_pos(c.getSeq_len());
                    c.addSpe_cha_ch(temp_cha - 'A');
                }
                if (!n_flag) {
                    if (temp_cha == 'N') {
                        c.addnCha_begin(n_letters_len);
//                        nCha_begin[_nCha_len] = n_letters_len;
                        n_letters_len = 0;
                        n_flag = true;
                    }
                }
                else {
                    if (temp_cha != 'N') {
                        c.addnCha_length(n_letters_len);
                        n_letters_len = 0;
                        n_flag = false;
                    }
                }
                n_letters_len++;
            }
        }
        if (!flag)
            c.addSeq_low_length(letters_len);

        if (n_flag)
            c.addnCha_length(n_letters_len);

        for (int i = c.getSpe_cha_len() - 1; i > 0; i--)
            c.setSpe_cha_pos_Byturn(i,c.getSpe_cha_pos_Byturn(i)-c.getSpe_cha_pos_Byturn(i-1));

        return c;
    }

    private static void kMerHashingConstruct(reference_type ref){
        //initialize the point array
        for (int i = 0; i < hashTableLen; i++)
            ref.setrefBucket_byturn(-1,i);
        int value = 0;
        int step_len = ref.getRef_code_len() - kMerLen + 1;

        //calculate the value of the first k-mer
        for (int k = kMerLen - 1; k >= 0; k--) {
            value <<= 2;
            value += integerCoding(ref.get_Ref_code_Byturn(k));
        }
        ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),0);
        ref.setrefBucket_byturn(0,value);

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;

        //calculate the value of the following k-mer using the last k-merf
        for (int i = 1; i < step_len; i++) {
            value >>= 2;
            value += (integerCoding(ref.get_Ref_code_Byturn(i + one_sub_str))) << shift_bit_num;
            ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),i);    //refLoc[i] record the list of same values
            ref.setrefBucket_byturn(i,value);
        }
        System.out.println("kMerHashingConstruct complete.");
    }

    private static int getNextPrime(int number) {
        int cur = number + 1;
        boolean prime = false;
        while (!prime)
        {
            prime = true;
            for (int i = 2; i < Math.sqrt(number) + 1; i++) {
                if (cur % i == 0) {
                    prime = false;
                    break;
                }
            }

            if (!prime) cur++;
        }
        return cur;
    }

    private static int integerCoding(char ch) { //encoding ACGT
        switch (ch) {
            case 'A': return 0;
            case 'C': return 1;
            case 'G': return 2;
            case 'T': return 3;
            default : return -1;
        }
    }

    private static void seqLowercaseMatching(compression_type tar,reference_type ref) {
        int start_position = 1;
        int _diff_low_len = 0;

        //initialize the diff_low_loc, diff_low_loc record the location of the same lowercase element
        int []diff_low_begin = new int[VEC_SIZE];
        int []diff_low_length = new int[VEC_SIZE];

        for (int i = 0; i < tar.getSeq_low_len(); i++) {
            //search from the start_position to the end
            for (int j = start_position; j < ref.getRef_low_len(); j++) {
                if ((tar.getSeq_low_begin_Byturn(i)== ref.get_Ref_low_begin_byturn(j))
                        && (tar.getSeq_low_length_Byturn(i) == ref.get_Ref_low_length_byturn(j))) {
                    tar.setLow_loc_Byturn(j,i);//low_loc[i] = j;
                    start_position = j + 1;
                    break;
                }
            }

            //search from the start_position to the begin
            if (tar.getLow_loc_Byturn(i) == 0) {
                for (int j = start_position - 1; j > 0; j--) {
                    if ((tar.getSeq_low_begin_Byturn(i) == ref.get_Ref_low_begin_byturn(j))
                            && (tar.getSeq_low_length_Byturn(i) == ref.get_Ref_low_length_byturn(j))) {
                        tar.setLow_loc_Byturn(j,i);
                        start_position = j + 1;
                        break;
                    }
                }
            }

            //record the mismatched information
            if (tar.getLow_loc_Byturn(i) == 0) {
                diff_low_begin[_diff_low_len] = tar.getSeq_low_begin_Byturn(i);
                diff_low_length[_diff_low_len++] = tar.getSeq_low_length_Byturn(i);
            }
        }
        tar.lowerInicial();
        tar.setDiff_low_len(_diff_low_len);
        tar.setDiff_low_begin(diff_low_begin);
        tar.setDiff_low_length(diff_low_length);
    }

/*    private static void saveSpeChaData(BufferedWriter wr, int _vec_len, int[]a, int[]b)throws IOException
    {
        int []flag=new int[26];
        int temp;
        for (int i = 0; i < 26; i++)
            flag[i] = -1;
        ArrayList<Integer> arr=new ArrayList<>();
        //fprintf(fp, "%d ", _vec_len);
        for (int i = 0; i < _vec_len; i++)
        {
            wr.write(a[i]);
            //fprintf(fp, "%d ", _vec[i].pos);
            temp = b[i];
            if (flag[temp] == -1)
            {
                arr.add(temp);
                flag[temp] = arr.size() - 1;
            }
        }

        int size = arr.size();
        wr.write(size);

        for (int i = 0; i < size; i++)
        {
            wr.write(arr.get(i));
            wr.flush();
        }

        if (size != 1)
        {
            int bit_num = (int) ceil(log(size) / log(2));
            int v_num = (int) floor(32.0 / bit_num);
            for (int i = 0; i < _vec_len; )
            {
                int v = 0;
                for (int j = 0; j < v_num && i < _vec_len; j++, i++)
                {
                    v <<= bit_num;
                    v += flag[b[i]];
                }
                wr.write(v);
                wr.flush();
            }
        }

    }*/

/*    private static void saveIdentifierData(List<String> id) {
        String[] _vec=new String[id.size()];
        //int []code={};
        ArrayList<Integer> code=new ArrayList<>();
        _vec[0]=id.get(0);
        String pre_str = id.get(0);
        int cnt = 1;
        for (int i = 1; i <id.size(); i++) {
            if ((id.get(i)).equals(pre_str))
                cnt++;
            else {
                code.add(cnt);
                _vec[i]=id.get(i);
                pre_str = id.get(i);
                cnt = 1;
            }
        }
        code.add(cnt);
        int code_len = code.size();

        id.clear();
        id.add(String.valueOf(code_len));
//        id.add(" ");
        for (int i = 0; i < code_len; i++) {
            id.add(String.valueOf(code.get(i)));
//            id.add(" ");
        }
//        id.add("\n");
        for (int i = 0; i < code_len; i++) {
            id.add(_vec[i]);
//            id.add(" ");
        }
//        id.add("\n");
    }*/

    private static List<Integer> runLengthCoding(int []vec ,int length, int tolerance) {
        //File fp =new File(filename);
        List<Integer> code=new ArrayList<>();
        if (length > 0)
        {
            code.add(vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++)
            {
                if (vec[i] - vec[i-1] == tolerance)
                    cnt++;
                else
                {
                    code.add(cnt);
                    code.add(vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);
        }
/*        int code_len = code.size();
        //PrintWriter wr = new PrintWriter(new FileWriter(fp));
        try {
            wr.write(code_len);
            for (int i: code) {
                wr.write(i+" ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        System.out.println("RunlenFinish");

        return code;
    }

    private static void saveOtherData(compression_type tar,reference_type ref,List<String> other) {
        //小写字符
        int flag = 0;
//            List<String> low = new ArrayList<>();//这种方式是否可行 还得探索！
        //保存小写信息：如果匹配程度大于50%，则记录LowVecMatched和DiffLowVec；否则直接写匹配之前的结果
        if (tar.getSeq_low_len() > 0 && ref.getRef_low_len() > 0) {
            seqLowercaseMatching(tar,ref);
            if ((2 * tar.getDiff_low_len()) < ref.getRef_low_len()) {
                flag = 1;
                other.add(String.valueOf(flag));
                List<Integer> loc = runLengthCoding(tar.getLow_loc(),tar.getSeq_low_len(),1);
                other.add(String.valueOf(loc.size()));
                for (Integer a: loc) {
                    other.add(String.valueOf(a));
                }
                other.add(String.valueOf(tar.getDiff_low_len()));
                for (int i = 0;i<=tar.getDiff_low_len();i++){
                    other.add(String.valueOf(tar.getSeq_low_begin_Byturn(i)));
                    other.add(String.valueOf(tar.getSeq_low_length_Byturn(i)));
                }
           }
        }
        if (flag == 0) {
            other.add(String.valueOf(flag));
//            other.add(" ");
            other.add(String.valueOf(tar.getSeq_low_len()));//长度必须先加
            for (int i = 0;i<tar.getSeq_low_len();i++){
                other.add(String.valueOf(tar.getSeq_low_begin_Byturn(i)));
                other.add(String.valueOf(tar.getSeq_low_length_Byturn(i)));
//                other.add(" ");
            }
        }
        tar.setDiff_low_len(0);
        tar.setDiff_low_begin(null);
        tar.setDiff_low_length(null);
        tar.setSeq_low_begin(null);
        tar.setSeq_low_length(null);
        tar.setLow_loc(null);

        //N字符
        other.add(String.valueOf(tar.getnCha_len()));
        for(int i = 0;i<tar.getnCha_len();i++){
            other.add(String.valueOf(tar.getnCha_begin_Byturn(i)));
            other.add(String.valueOf(tar.getnCha_length_Byturn(i)));
        }
        tar.setnCha_begin(null);
        tar.setnCha_length(null);

        //特殊字符
        other.add(String.valueOf(tar.getSpe_cha_len()));
        if(tar.getSpe_cha_len()>0){
//            saveSpeChaData();
            for(int i = 0;i<tar.getSpe_cha_len();i++){
                other.add(String.valueOf(tar.getSpe_cha_pos_Byturn(i)));
                other.add(String.valueOf(tar.getSpe_cha_ch_Byturn(i)));
            }
        }
        tar.setSpe_cha_pos(null);
        tar.setSpe_cha_ch(null);
//        other.add(" ");
    }

    private static List<MatchEntry> codeFirstMatch(compression_type tar, reference_type ref) {
        int pre_pos = 0;
        int min_rep_len = 15;
        int step_len = tar.getSeq_len() - kMerLen + 1;
        int max_length, max_k;
        int i, id, k, ref_idx, tar_idx, length, cur_pos, tar_value;
        StringBuilder mismatched_str = new StringBuilder();
        List<MatchEntry> mr = new ArrayList<>();
        //mismatched_str.reserve(10240);
        MatchEntry me = null;
        //matchResult.reserve(VEC_SIZE);
        for (i = 0; i < step_len; i++) {
            tar_value = 0;
            //calculate the hash value of the first k-mer
            for (k = kMerLen - 1; k >= 0; k--) {
                tar_value <<= 2;
                tar_value += integerCoding(tar.getSeq_code_byturn(i+k));
            }

            id = ref.getRefBucket_Byturn(tar_value);
            if (id > -1) {                      //there is a same k-mer in ref_seq_code
                max_length = -1;
                max_k = -1;
                //search the longest match in the linked list
                for (k = id; k != -1; k = ref.getrefLoc_byturn(k)) {
                    ref_idx = k + kMerLen;
                    tar_idx = i + kMerLen;
                    length = kMerLen;

                    while (ref_idx < ref.getRef_code_len() && tar_idx < tar.getSeq_len() &&
                            ref.get_Ref_code_Byturn(ref_idx++) == tar.getSeq_code_byturn(tar_idx++))
                        length++;

                    if (length >= min_rep_len && length > max_length) {
                        max_length = length;
                        max_k = k;
                    }
                }
                //exist a k-mer, its length is larger then min_rep_len
                if (max_length > -1) {
                    me= new MatchEntry();
                    //then save matched information
                    cur_pos = max_k - pre_pos;      //delta-coding for cur_pos
                    me.setPos(cur_pos);
                    me.setLength(max_length - min_rep_len);
                    me.setMisStr(mismatched_str.toString());
                    mr.add(me);
                    i += max_length;
                    pre_pos = max_k + max_length;
                    mismatched_str.delete(0,mismatched_str.length());
                    if (i < tar.getSeq_len())
                        mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;//mismatched_str存储的是0123数字字符
                    continue;
                }
            }
            mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
        }
        if (i < tar.getSeq_len()) {
            for (; i < tar.getSeq_len(); i++)
                mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
            me= new MatchEntry();
            me.setPos(0);
            me.setLength(-min_rep_len);                 //no match information, not 0 ,is -min_rep_len;
            me.setMisStr(mismatched_str.toString());
            mr.add(me);
        }
//        System.out.println("seqCodeMatching complete. MatchResult size: "+mr.size());
        tar.setSeq_code(null);
        return mr;
    }

    //计算MatchEntry的hash值
    private static int getHashValue(MatchEntry me) {
        int result = 0;
        for (int i = 0; i < me.getMisStr().length(); i++) {
            result += me.getMisStr().charAt(i) * 92083;
        }
        result += me.getPos() * 69061 + me.getLength() * 51787;
        result %= getNextPrime(1<<20);
        return result;
    }

    //计算matchResult的hash值
    private static void matchResultHashConstruct(List<MatchEntry> matchResult,Map<Integer,int[]> seqBucketVec,Map<Integer,List<Integer>> seqLocVec,int Num) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(VEC_SIZE);
        int []seqBucket = new int[seqBucketLen];
        for (int i = 0; i < seqBucketLen; i++) {
            seqBucket[i] = -1;
        }

        hashValue1 = getHashValue(matchResult.get(0));  //比较getHashValue
        if (matchResult.size() < 2) {
            hashValue2 = 0;
        } else {
            hashValue2 = getHashValue(matchResult.get(1));
        }
        hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < matchResult.size() - 1; i++) {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(matchResult.get(i + 1));
            hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }
        seqLocVec.put(Num,seqLoc);
        seqBucketVec.put(Num,seqBucket);
    }

    private static int getMatchLength(List <MatchEntry> ref_me, int ref_idx, List <MatchEntry> tar_me, int tar_idx) {
        int length = 0;
        while (ref_idx < ref_me.size() && tar_idx < tar_me.size() && compareMatchEntry(ref_me.get(ref_idx++), tar_me.get(tar_idx++)))
            length++;
        return length;
    }

    private static Boolean compareMatchEntry(MatchEntry ref, MatchEntry tar) {
        return  ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr());
    }

    private static void saveMatchEntry(List<String> list, MatchEntry matchEntry) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            list.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        list.add(sbf.toString());
/*        list.add(String.valueOf(matchEntry.getLength()));
        list.add(" ");*/
    }

    private static void codeSecondMatch( List<MatchEntry> _mr, int seqNum, Map<Integer,int[]> seqBucket_vec,
                                         Map<Integer,List<Integer>> seqLoc_vec , Map<Integer,List<MatchEntry>> matchResult_vec,
                                         List<String> list) {
        int hashValue;
        int pre_seq_id=1;
        int max_pos=0, pre_pos=0, delta_pos, length, max_length, delta_length, seq_id=0, delta_seq_id;
        int id, pos, secondMatchTotalLength=0;
        int i;
        StringBuilder sbt = new StringBuilder();
//        System.out.println("sec match input length"+seqNum+" "+_mr.size());
        ArrayList<MatchEntry> misMatchEntry = new ArrayList<>();
        for (i = 0; i < _mr.size()-1; i++) {

            //构建这个matchentry的hash 表 ，每一个matchentry都要编码并且与
            if(_mr.size()<2) hashValue = abs(getHashValue(_mr.get(i))) % seqBucketLen;//一般这种情况不太会发生
            else hashValue = abs(getHashValue(_mr.get(i)) + getHashValue(_mr.get(i+1))) % seqBucketLen;
            max_length = 0;
            //寻找相同序列
            for (int m = 0; m < min( seqNum-1, 45); m++) {
                id = seqBucket_vec.get(m)[hashValue];//寻找出参考序列组的前m个编码序列依次匹配
                if (id!=-1) {
                    for (pos = id; pos!=-1; pos = seqLoc_vec.get(m).get(pos)) {
                        length = getMatchLength(matchResult_vec.get(m), pos, _mr, i);
                        if (length > 1 && length > max_length) {
                            seq_id = m + 1;  //在seqBucket是第m个，但是在seqName里面其实是第m+1个
                            max_pos = pos;
                            max_length = length;//赋值则说明拥有最大长度的序列
                        }
                    }
//                    System.out.println(max_length);
                }
            }
            //达到最长匹配长度后，保存
            if (max_length!=0) {
                delta_seq_id = seq_id - pre_seq_id;//delta encoding, 把seq_id变成0
                delta_length = max_length - 2;//delta encoding, 减小length的数值大小，因为最小替换长度是2
                delta_pos = max_pos - pre_pos;//delta encoding, 减小pos的数值大小
                pre_seq_id = seq_id;
                pre_pos = max_pos + max_length;
                secondMatchTotalLength += max_length;

                //firstly save mismatched matchentry！
                if (!misMatchEntry.isEmpty()) {
                    for (MatchEntry k:misMatchEntry) {
                        saveMatchEntry(list,k);
                    }
                    misMatchEntry.clear();  //只是清空vector中的元素，并不释放内存，如果要释放内存，需要用swap()函数
                }
                //secondly save matched matchentry！
                sbt.append(delta_seq_id).append(' ').append(delta_pos).append(' ').append(delta_length);
                list.add(sbt.toString());
                sbt.delete(0,sbt.length());
                i += max_length - 1;//移动i的位置
            }
            else {
                misMatchEntry.add(_mr.get(i));
            }
        }
        //剩下没存完的 存掉
        if (i == _mr.size()-1)  misMatchEntry.add(_mr.get(i));
        if (!misMatchEntry.isEmpty()) {
            for (MatchEntry matchEntry : misMatchEntry) saveMatchEntry(list, matchEntry);
            misMatchEntry.clear();
        }
        System.out.println(seqNum+" code second match complete. The second match length: "+secondMatchTotalLength);
    }

    public static void compress(String ref_file,String tar_file,String out_path){
        SparkConf sparkConf = new SparkConf();
        //实现kryo序列化方式并注册需要使用序列化的类
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        // 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
        // 但是如果设置为local[*]则代表，在本地使用最大线程运行
//        String masterurl = "spark://10.166.38.112:7077";
        sparkConf.set("spark.kryo.registrator", mykryo.class.getName()).setAppName("geneCompress").setMaster("yarn");
        sparkConf.set("spark.kryoserializer.buffer.max","2000").set("spark.driver.maxResultSize", "6g").set("spark.shuffle.sort.bypassMergeThreshold","20");
        /*sparkConf.set("spark.default.parallelism", "20");*/
        sparkConf.set("spark.default.parallelism", "40").set("spark.shuffle.file.buffer","3000").set("spark.reducer.maxSizeInFlight", "1000")/*.set("spark.shuffle.io.maxRetries", "6")*/;
        sparkConf.set("spark.broadcast.blockSize", "256m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //hadoop的那一套参数在JavaSparkContext里也能设置！
        //最大分片数调整为256M，这样可以涵盖所有的基因文件，因为输入分片最大不会跨越文件。一个分片对应一个partition！
        jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize","268435456");

//        Path path1 = new Path(tar_file);
        Path path2 = new Path(out_path);
        /*
            使用FileSystemAPI
            读取参考序列以及清理输出位置
        * */
        try {
            FileSystem fs =FileSystem.get(jsc.hadoopConfiguration());
            if(fs.exists(path2)){
                fs.delete(path2,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        reference_type ref = new reference_type();
        createRefBroadcast(ref,ref_file);
        //对参考序列进行广播变量
        final Broadcast<reference_type> referenceTypeBroadcast = jsc.broadcast(ref);

        //一次压缩参考序列释放
        referenceTypeBroadcast.unpersist();
        JavaRDD<String> tar_rdd = jsc.textFile(tar_file); //路径为文件夹在HDFS上的全路径
        //创建待压缩序列RDD，这个RDD需要缓存！
        JavaPairRDD<Text,Text> ta_rdd = jsc.newAPIHadoopFile(tar_file, KeyValueTextInputFormat.class, Text.class ,Text.class ,jsc.hadoopConfiguration());
        JavaRDD<Tuple3<Integer,List<MatchEntry>,List<String>>> first_match = ta_rdd.mapPartitions(s1->{
            /*
             * 重要！JavaRDD FlatMapFunction需要返回一个迭代器，这是出于按行输入的考虑，但我们不需要迭代器
             * 我们的需求是只需要返回一个compression_type对象即可，但为了完成这个函数，必须把一个对象封装成迭代器的方式输出！
             * 常规方式就是，使用ArrayList
             * */
            List<compression_type> list = new ArrayList<>();
            //封装进去 完成输出
            list.add(createSeqRDD(s1));
            return list.iterator();
        }).mapPartitionsWithIndex((v1,v2)->{
            List<Tuple3<Integer,List<MatchEntry>,List<String>>> li = new ArrayList<>();
            List<String> list = new ArrayList<>();
            Tuple3<Integer,List<MatchEntry>,List<String>> tu2 =null;
            while(v2.hasNext()&&referenceTypeBroadcast.value()!=null) {
//                reference_type ref1 = referenceTypeBroadcast.getValue();
                compression_type tar = v2.next();
//                list.add(String.valueOf(v1));
//                list.add(tar_rdd.name());
                list.add(tar.identifier);
                list.add(String.valueOf(tar.line));
                saveOtherData(tar,referenceTypeBroadcast.value(),list);
                tu2 = new Tuple3<>(v1, codeFirstMatch(tar, referenceTypeBroadcast.value()),list);
            }
            li.add(tu2);
            return li.iterator();
        },true).persist(MEMORY_ONLY_SER);//需要把它持久化到内存！
 /*       JavaRDD<Tuple3<Integer,List<MatchEntry>,List<String>>> read_tar = tar_rdd.mapPartitions(s-> {
                *//*
                 * 重要！JavaRDD FlatMapFunction需要返回一个迭代器，这是出于按行输入的考虑，但我们不需要迭代器
                 * 我们的需求是只需要返回一个compression_type对象即可，但为了完成这个函数，必须把一个对象封装成迭代器的方式输出！
                 * 常规方式就是，使用ArrayList
                 * *//*
                List<compression_type> list = new ArrayList<>();
                //封装进去 完成输出
                list.add(createSeqRDD(s));
                return list.iterator();
            }
        ).mapPartitionsWithIndex((v1,v2)->{
            List<Tuple3<Integer,List<MatchEntry>,List<String>>> li = new ArrayList<>();
            List<String> list = new ArrayList<>();
            Tuple3<Integer,List<MatchEntry>,List<String>> tu2 =null;
            while(v2.hasNext()&&referenceTypeBroadcast.value()!=null) {
//                reference_type ref1 = referenceTypeBroadcast.getValue();
                compression_type tar = v2.next();
//                list.add(String.valueOf(v1));
                FileSystem.get(;
                System.out.println(jsc.hadoopConfiguration().getFile());
//                list.add(tar_rdd.name());
                list.add(tar.identifier);
                list.add(String.valueOf(tar.line));
                saveOtherData(tar,referenceTypeBroadcast.value(),list);
                tu2 = new Tuple3<>(v1, codeFirstMatch(tar, referenceTypeBroadcast.value()),list);
            }
            li.add(tu2);
            return li.iterator();
        },true).persist(MEMORY_ONLY_SER);//需要把它持久化到内存！;*/

/*        //直接将其他信息输出
        //保存小写字符String类型
        JavaRDD<String> other = read_tar.mapPartitions(s->{
            List<String> list = new ArrayList<>();
            compression_type tar = s.next();
            list.add(tar.identifier);
            list.add(String.valueOf(tar.line));
            saveOtherData(tar,referenceTypeBroadcast.value(),list);
            return list.iterator();
        })*//*.saveAsTextFile("J:\\gene\\output\\spark\\out1.fa")*//*;
        other.saveAsTextFile(path1.toString());*/

/*        JavaRDD<Tuple3<Integer,List<MatchEntry>,List<String>>> first_Res = read_tar.mapPartitionsWithIndex((v1,v2)->{
            List<Tuple3<Integer,List<MatchEntry>,List<String>>> li = new ArrayList<>();
            List<String> list = new ArrayList<>();
            Tuple3<Integer,List<MatchEntry>,List<String>> tu2 =null;
            while(v2.hasNext()) {
                compression_type tar = v2.next();
                list.add(tar.identifier);
                list.add(String.valueOf(tar.line));
                saveOtherData(tar,referenceTypeBroadcast.value(),list);
                tu2 = new Tuple3<>(v1, codeFirstMatch(v2.next(), referenceTypeBroadcast.value()),list);
            }
            li.add(tu2);
            return li.iterator();
        },true).persist(MEMORY_ONLY_SER);//需要把它持久化到内存！*/

        //identifier & line 信息存储信息存储 shuffle
/*        JavaRDD<String> identifier = read_tar.map(s->new Tuple2<>(s.identifier,s.line)).repartition(1).mapPartitions(s->{
            List<String> id = new ArrayList<>();
            int []arr = new int[2000];
            Tuple2<String,Integer> tu;
            int i = 0;
            while (s.hasNext()){
                tu = s.next();
                id.add(tu._1);
                arr[i++] = tu._2;
            }
            saveIdentifierData(id);
            List<Integer> o1 = runLengthCoding(arr,i,0);
            id.add(String.valueOf(o1.size()));
            for (Integer a:o1) {
                id.add(String.valueOf(a));
            }
            return id.iterator();
        });*/
        //一次匹配压缩-----v2！
        /*最后的形式为每个分区中有一个map<Integer,List<MatchEntry>> Integer为分区号，List<MatchEntry>为一次匹配压缩结果
        需要产生两个依赖！
        * */
//        read_tar.unpersist();//不需要缓存了
        JavaRDD<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec = first_match.filter(s->s._1()<=45)
                .coalesce(1,true).mapPartitions(s->{
                    List<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> list = new ArrayList<>();
                    Map<Integer,List<MatchEntry>> MatchList = new HashMap<>(47);
                    Map<Integer,List<Integer>> seqLoc = new HashMap<>(47);
                    Map<Integer,int[]> seqBucket = new HashMap<>(47);
/*              List<Tuple3<List<MatchEntry>,int[],List<Integer>>> li = new ArrayList<>();
                int[] seqBucketVec = new int[seqBucketLen];
                List<Integer> seqLocVec = new ArrayList<>(); //存储所有序列的冲突元素*/
                while (s.hasNext()){
                    Tuple3<Integer,List<MatchEntry>,List<String>> l = s.next();

                    matchResultHashConstruct(l._2(),seqBucket,seqLoc,l._1());
                    MatchList.put(l._1(),l._2());
                }
//                    System.out.println(MatchList.size());
                list.add(new Tuple3<>(MatchList,seqBucket,seqLoc));
                return list.iterator();
        });
        //收集用于索引的List<MatchEntry>
/*        List<List<MatchEntry>> sec1 = new ArrayList<>();
                sec.foreachPartition(s->sec1.add(s.next()));*/

        //收集编码后的List<MatchEntry>
        Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>> ref2 = sec.first();
        ref.set_Ref_code(null);
        ref.setRefLoc(null);
        ref.setRefBucket(null);
        final Broadcast<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec_ref = jsc.broadcast(ref2);
/*
        JavaRDD<Tuple2<List<int[]>,List<List<Integer>>>> sec2 = sec.map(s->{
            List<int[]> seqBucketVec = new ArrayList<>();
            List<List<Integer>> seqLocVec = new ArrayList<>(); //存储所有序列的冲突元素
            for (List<MatchEntry> a: s) {
                matchResultHashConstruct(a,seqBucketVec,seqLocVec);
            }
            return new Tuple2<>(seqBucketVec,seqLocVec);
        });
*/
//        final Broadcast<Tuple2<List<int[]>,List<List<Integer>>>> sec_ref2 = jsc.broadcast(sec2.first());

        first_match.mapPartitions(s->{
            List<String> list = new ArrayList<>();
//            Tuple3<Integer,List<MatchEntry>,List<String>> tu3;
            Tuple3<Integer,List<MatchEntry>,List<String>> tar;
            //根据分区号来进行筛选
//            System.out.println(sec_ref.value()._1().size());
//            System.out.println(sec_ref.value()._2().size()+" "+sec_ref.value()._2().get(0).length);
//            System.out.println(sec_ref.value()._3().size()+" "+sec_ref.value()._3().get(0).size());
            while (s.hasNext()&&sec_ref.value()!=null) {
                tar = s.next();
                list = tar._3();
//                Collections.copy(list,tu3._3());
                if(tar._1() == 0){
                    for (int i = 0; i < tar._2().size(); i++) {
                        saveMatchEntry(list, tar._2().get(i));
                    }
                }else
                    codeSecondMatch(tar._2(),tar._1()+1,
                            sec_ref.value()._2(),sec_ref.value()._3(),sec_ref.value()._1(),list);//传递的是分区号
            }
            return list.iterator();
        }).saveAsTextFile(path2.toString());
        sec_ref.unpersist();
        first_match.unpersist();
    }
}

