package geneCompress;


import org.apache.spark.sql.execution.aggregate.HashMapGenerator;
import pojo.MatchEntry;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.*;

public class compress {

    private static int MAX_SEQ_NUM = 2000;//maximum sequence number
    private static int MAX_CHA_NUM = 1 << 28;//maximum length of a chromosome
    private static int LINE_CHA_NUM =200;
    private static int PERCENT = 10; //the percentage of compressed sequence number uses as reference
    private static int kMerLen = 12; //the length of k-mer
    private static int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static int hashTableLen = 1 << kmer_bit_num; // length of hash table
    private static int VEC_SIZE = 1 <<20; //length for other character arrays
    private static int min_rep_len = 35;   //minimum replace length, matched string length exceeds min_rep_len, saved as matched information
    private static long startTime;

    private static String identifier;
    private static int lineWidth, ref_code_len, seq_code_len, ref_low_len, seq_low_len, diff_low_len, nCha_len, spe_cha_len, seqNumber, seqBucketLen;
    private static int sec_seq_num; //the referenced sequence number used for second compress
    private static char ref_code[]=new char[MAX_CHA_NUM];
    private static char seq_code[]= new char[MAX_CHA_NUM];//mismatched subsequence
    private static int refLoc[]= new int[MAX_CHA_NUM]; //reference hash location
    private static int refBucket[]= new int[hashTableLen]; //reference hash bucket
    private static int low_loc[]= new int[VEC_SIZE]; //lowercase tuple location

    private static int ref_low_begin[];
    private static int ref_low_length[];
    private static int seq_low_begin[];
    private static int seq_low_length[];
    private static int diff_low_begin[];
    private static int diff_low_length[];
    private static int nCha_begin[];
    private static int nCha_length[];
    private static int spe_cha_pos[];
    private static int spe_cha_ch[];

    private static List<String> seqName = new ArrayList<>();;
    private static List<String> identifier_vec;
    private static List<Integer> lineWidth_vec;
    private static List <MatchEntry> matchResult;                 //存储一个序列的初次匹配结果
    private static List <MatchEntry> misMatchEntry;               //存储二次匹配时未匹配matchentry
    private static List <ArrayList <MatchEntry> > matchResult_vec;
    private static List <ArrayList<Integer> > seqLoc_vec;
    private static List<int []> seqBucket_vec;

    private static void initial()   // allocate memory
    {
        ref_low_begin = new int[VEC_SIZE];
        ref_low_length = new int[VEC_SIZE];
        seq_low_begin = new int[VEC_SIZE];
        seq_low_length = new int[VEC_SIZE];
        diff_low_begin = new int[VEC_SIZE];
        diff_low_length = new int[VEC_SIZE];
        nCha_begin = new int[VEC_SIZE];
        nCha_length = new int[VEC_SIZE];
        spe_cha_ch = new int[VEC_SIZE];
        spe_cha_pos = new int[VEC_SIZE];

        identifier_vec = new ArrayList<>(seqNumber);
        lineWidth_vec = new ArrayList<>(seqNumber);
        matchResult = new ArrayList<>();
        misMatchEntry = new ArrayList<>();
        matchResult_vec=new ArrayList<>();
        seqLoc_vec=new ArrayList<>(seqNumber);
        seqBucket_vec=new ArrayList<>(seqNumber);
        //lineWidth_vec.reverse();
        //seqBucket_vec.reserve(seqNumber);
        //seqLoc_vec.reserve(seqNumber);
    }

    private static int integerCoding(char ch) { //encoding ACGT
        switch (ch) {
            case 'A': return 0;
            case 'C': return 1;
            case 'G': return 2;
            case 'T': return 3;
            default : return 4;
        }
    }

    /**
     * 在文件中读取文件名称，将其存储至文件名数组。
     */

    private static void readFile(String filename) {
        File fp =new File(filename);
        String temp_name;
        //the length of filename
        BufferedReader br;
        try {
            br=new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(fp))));
            while ((temp_name=br.readLine())!=null)
            {
                seqName.add(temp_name);//放在对象末尾
            }
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        seqNumber = seqName.size();
    }

    private static void referenceSequenceExtraction(String str_referenceName){
        int _seq_code_len = 0, _ref_low_len = 1, letters_len = 0;//record lowercase from 1, diff_lowercase_loc[i]=0 means mismatching
        char temp_cha;
        boolean flag = true;
        String str;
        char cha[];      //the content of one line
        File fp=new File(str_referenceName);
        BufferedReader br;

        try {
            br=new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(fp))));
            br.readLine();
            while ((str=br.readLine())!=null)
            {
                cha=str.toCharArray();
                for (int i=0;i<cha.length;i++){
                    temp_cha=cha[i];
                    if(Character.isLowerCase(temp_cha)){
                        if (flag) //previous is upper case
                        {
                            flag = false; //change status of flag
                            ref_low_begin[_ref_low_len] = letters_len;
                            letters_len = 0;
                        }
                        temp_cha = Character.toUpperCase(temp_cha);
                    }
                    else {
                        if (!flag)  //previous is lower case
                        {
                            flag = true;
                            ref_low_length[_ref_low_len++] = letters_len;
                            letters_len = 0;
                        }
                    }
                    if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T')
                        ref_code[_seq_code_len++] = temp_cha;
                    letters_len++;
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!flag)  //if flag=false, don't forget record the length
            ref_low_length[_ref_low_len++] = letters_len;

        ref_code_len = _seq_code_len;
        ref_low_len = _ref_low_len - 1;
        System.out.println("Extraction of reference sequence complete. Reference code length: %d. Lowercase length: %d.\n"+ ref_code_len+"\t"+ref_low_len);
    }


    private static void targetSequenceExtraction(String str_sequenceName)//complete
    {
        //const char* cSequenceName = str_sequenceName.c_str();
        File fp=new File(str_sequenceName);
        String str;
        int _seq_code_len =0;
        int _seq_low_len = 0;
        int _nCha_len = 0;
        int _spe_cha_len = 0;
        int letters_len = 0, n_letters_len = 0;
        boolean flag = true, n_flag = false;
        char cha[];      //the content of one line
        char temp_cha;
        BufferedReader br;

        try {
            br=new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(fp))));
            //get the identifier
            identifier=br.readLine();
            identifier_vec.add(identifier);

            br.mark(300);
            str=br.readLine();
            lineWidth=str.length();
            lineWidth_vec.add(lineWidth);
            br.reset();
            while ((str=br.readLine())!=null){
                cha=str.toCharArray();
                //有不满行！
                for (int i = 0; i < cha.length; i++){
                    temp_cha = cha[i];
                    if (Character.isLowerCase(temp_cha)) {
                        if (flag) //previous is upper case
                        {
                            flag = false;
                            seq_low_begin[_seq_low_len] = letters_len;
                            letters_len = 0;
                        }
                        temp_cha = Character.toUpperCase(temp_cha);
                    }
                    else
                    {
                        if (Character.isLowerCase(temp_cha))
                        {
                            if (!flag)
                            {
                                flag = true;
                                seq_low_length[_seq_low_len++] = letters_len;
                                letters_len = 0;
                            }
                        }
                    }
                    letters_len++;

                    //temp_cha is an upper letter
                    if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T')
                        seq_code[_seq_code_len++] = temp_cha;
                    else if (temp_cha != 'N')
                    {
                        spe_cha_pos[_spe_cha_len] = _seq_code_len;
                        spe_cha_ch[_spe_cha_len++] = temp_cha - 'A';
                    }
                    if (!n_flag)
                    {
                        if (temp_cha == 'N')
                        {
                            nCha_begin[_nCha_len] = n_letters_len;
                            n_letters_len = 0;
                            n_flag = true;
                        }
                    }
                    else
                    {
                        if (temp_cha != 'N')
                        {
                            nCha_length[_nCha_len++] = n_letters_len;
                            n_letters_len = 0;
                            n_flag = false;
                        }
                    }
                    n_letters_len++;
                }
            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //get the lineWidth
       /* if (fscanf(fp, "%s", cha) != EOF)
            lineWidth = strlen(cha);
        lineWidth_vec.push_back(lineWidth);
        fseek(fp, -1L * (lineWidth+1), SEEK_CUR);//从当前位置偏移-(lineWidth+1)个字节，即指针回到行首,如果仅偏移70个字节是回不到行首的，因为第一行实际上是71个字节，还有一个‘\n’符。
*/
        if (!flag)
            seq_low_length[_seq_low_len++] = letters_len;

        if (n_flag)
            nCha_length[_nCha_len++] = n_letters_len;

        for (int i = _spe_cha_len - 1; i > 0; i--)
            spe_cha_pos[i] -= spe_cha_pos[i-1];

        seq_code_len=_seq_code_len;
        seq_low_len=_seq_low_len;
        nCha_len=_nCha_len;
        spe_cha_len=_spe_cha_len;

        System.out.println("Extraction of sequence %s complete."+str_sequenceName);
        System.out.println("Code length: %d. N character length: %d. Lowercase length: %d. Special length: %d\n"+ _seq_code_len+"\t"+ _nCha_len+"\t"+ _seq_low_len+"\t"+ _spe_cha_len);
    }


    private static void kMerHashingConstruct()//complete
    {
        //initialize the point array
        for (int i = 0; i < hashTableLen; i++)
            refBucket[i] = -1;
        int value = 0;
        int step_len = ref_code_len - kMerLen + 1;

        //calculate the value of the first k-mer
        for (int k = kMerLen - 1; k >= 0; k--) {
            value <<= 2;
            value += integerCoding(ref_code[k]);
        }
        refLoc[0] = refBucket[value];
        refBucket[value] = 0;

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;

        //calculate the value of the following k-mer using the last k-mer
        for (int i = 1; i < step_len; i++) {
            value >>= 2;
            value += (integerCoding(ref_code[i + one_sub_str])) << shift_bit_num;
            refLoc[i] = refBucket[value];    //refLoc[i] record the list of same values
            refBucket[value] = i;
        }
        System.out.println("kMerHashingConstruct complete.");
    }

    private static void codeFirstMatch(char[]tar_seq_code, int tar_seq_len, ArrayList<MatchEntry> mr)
    {
        int pre_pos = 0;
        int step_len = tar_seq_len - kMerLen + 1;
        int max_length, max_k;
        int i, id, k, ref_idx, tar_idx, length, cur_pos, tar_value;
        StringBuilder mismatched_str = new StringBuilder();
        //mismatched_str.reserve(10240);
        MatchEntry me ;
        //matchResult.reserve(VEC_SIZE);
        for (i = 0; i < step_len; i++)
        {
            tar_value = 0;
            //calculate the hash value of the first k-mer
            for (k = kMerLen - 1; k >= 0; k--)
            {
                tar_value <<= 2;
                tar_value += integerCoding(tar_seq_code[i + k]);
            }

            id = refBucket[tar_value];
            if (id > -1)
            {                      //there is a same k-mer in ref_seq_code
                max_length = -1;
                max_k = -1;
                //search the longest match in the linked list
                for (k = id; k != -1; k = refLoc[k])
                {
                    ref_idx = k + kMerLen;
                    tar_idx = i + kMerLen;
                    length = kMerLen;

                    while (ref_idx < ref_code_len && tar_idx < tar_seq_len && ref_code[ref_idx++] == tar_seq_code[tar_idx++])
                        length++;

                    if (length >= min_rep_len && length > max_length)
                    {
                        max_length = length;
                        max_k = k;
                    }
                }

                if (max_length > -1) //exist a k-mer, its length is larger then min_rep_len
                {

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
                    if (i < tar_seq_len)
                        mismatched_str.append(integerCoding(tar_seq_code[i])) ;//mismatched_str存储的是0123数字字符
                    continue;
                }
            }
            mismatched_str.append(integerCoding(tar_seq_code[i])) ;
        }
        if (i < tar_seq_len)
        {
            for (; i < tar_seq_len; i++)
                mismatched_str.append(integerCoding(tar_seq_code[i])) ;
            me= new MatchEntry();
            me.setPos(0);
            me.setLength(-min_rep_len);                 //no match information, not 0 ,is -min_rep_len;
            me.setMisStr(mismatched_str.toString());
            mr.add(me);
        }
        System.out.println("seqCodeMatching complete. MatchResult size: "+mr.size());
        //gettimeofday(&c1_end, NULL);
        //c1_timer = 1000000 * (c1_end.tv_sec - c1_start.tv_sec) + c1_end.tv_usec - c1_start.tv_usec;
        //printf("codeFirstMatch time = %lf ms; %lf s\n", c1_timer / 1000.0, c1_timer / 1000.0 / 1000.0);
    }

    //Diff_low_len
    private static void seqLowercaseMatching(int _seq_low_len)
    {
        int start_position = 1;
        int _diff_low_len = 0;

        //initialize the diff_low_loc, diff_low_loc record the location of the same lowercase element
        //memset(low_loc, 0, sizeof(int)*_seq_low_len);
        low_loc=null;
        low_loc=new int[VEC_SIZE];
        for (int i = 0; i < _seq_low_len; i++)
        {
            //search from the start_position to the end
            for (int j = start_position; j < ref_low_len; j++)
            {
                if ((seq_low_begin[i] == ref_low_begin[j]) && (seq_low_length[i] == ref_low_length[j]))
                {
                    low_loc[i] = j;
                    start_position = j + 1;
                    break;
                }
            }

            //search from the start_position to the begin
            if (low_loc[i] == 0)
            {
                for (int j = start_position - 1; j > 0; j--) {
                    if ((seq_low_begin[i] == ref_low_begin[j]) && (seq_low_length[i] == ref_low_length[j]))
                    {
                        low_loc[i] = j;
                        start_position = j + 1;
                        break;
                    }
                }
            }

            //record the mismatched information
            if (low_loc[i] == 0)
            {
                diff_low_begin[_diff_low_len] = seq_low_begin[i];
                diff_low_length[_diff_low_len++] = seq_low_length[i];
            }
        }
        diff_low_len = _diff_low_len;
    }

    private static void runLengthCoding(BufferedWriter wr, int []vec ,int length, int tolerance)
    {
        //File fp =new File(filename);
        ArrayList<Integer> code=new ArrayList<>();
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
        int code_len = code.size();

        //PrintWriter wr = new PrintWriter(new FileWriter(fp));
        try {
            wr.write(code_len);
            for (int i: code) {
                wr.write(i+" ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("RunlenFinish");
    }

    private static void saveIdentifierData(BufferedWriter wr) throws IOException
    {
        String[] _vec=new String[identifier_vec.size()];
        //int []code={};
        ArrayList<Integer> code=new ArrayList<>();

        _vec[0]=identifier_vec.get(0);

        String pre_str = identifier_vec.get(0);
        int cnt = 1;
        for (int i = 1; i <identifier_vec.size(); i++)
        {
            if ((identifier_vec.get(i)).equals(pre_str))
                cnt++;
            else
            {
                code.add(cnt);
                _vec[i]=identifier_vec.get(i);
                pre_str = identifier_vec.get(i);
                cnt = 1;
            }
        }
        code.add(cnt);
        int code_len = code.size();
        //PrintWriter wr = new PrintWriter(new FileWriter(fp));

        wr.write(code_len);
        for (int i = 0; i < code_len; i++)
        {
            wr.write(code.get(i));
            //wr.flush();
        }

        wr.write("\n");
        for (int i = 0; i < code_len; i++)
        {
            wr.write(_vec[i]);//没必要加换行符，本身自带换行符
            //wr.flush();
        }
        wr.write("\n");
    }

    private static void savePositionRangeData(BufferedWriter wr, int _vec_len, int[]a,int[]b) throws IOException {
        //PrintWriter wr = new PrintWriter(new FileWriter(fp));
        wr.write(_vec_len);
        for (int i = 0; i < _vec_len; i++) {
            wr.write(a[i]+" ");
            wr.write(b[i]+" ");
            wr.flush();
        }
        // fprintf(fp, "%d %d ", _vec[i].begin, _vec[i].length);
    }

    private static void saveSpeChaData(BufferedWriter wr, int _vec_len, int[]a, int[]b)throws IOException
    {
        int flag[]=new int[26];
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

    }

    private static void saveOtherData(BufferedWriter wr, int _seq_low_len, int _nCha_len, int _spe_cha_len) throws IOException {
        int flag = 0;
        if (_seq_low_len > 0 && ref_low_len > 0)
        {
            seqLowercaseMatching(_seq_low_len);
            if (2 * diff_low_len < _seq_low_len)
            {
                flag = 1;

                wr.write(flag);
                wr.flush();
                //fprintf(fp, "%d ", flag);
                runLengthCoding(wr, low_loc,_seq_low_len, 1);
                //printf("the length of diff_low_len is : %d ", diff_low_len);
                savePositionRangeData(wr, diff_low_len, diff_low_begin,diff_low_length);
            }
        }
        if(flag!=1)
        {
            wr.write(flag);
            wr.flush();
            //fprintf(fp, "%d ", flag);
            savePositionRangeData(wr, _seq_low_len, seq_low_begin,seq_low_length);
        }

        savePositionRangeData(wr, _nCha_len, nCha_begin,nCha_length);

        //save special character information
        wr.write(_spe_cha_len);
        wr.flush();
        if (_spe_cha_len > 0)
        {
            saveSpeChaData(wr, _spe_cha_len, spe_cha_pos,spe_cha_ch);
        }
        wr.write("\n");
        //fprintf(fp,"\n");//The end of other data
        System.out.println("saveOtherData complete.");
    }

    private static void saveMatchEntry(BufferedWriter wr, MatchEntry _me)throws IOException
    {
        if (_me.getMisStr()!=null)
        {
            wr.write(_me.getMisStr());
            //wr.flush();
        }
        wr.write(_me.getPos());
        //wr.flush();
        wr.write(_me.getLength());
    }

    private static void saveFirstMatchResult(BufferedWriter wr, ArrayList <MatchEntry> _mr) throws IOException
    {
        for (MatchEntry i:_mr) {
            saveMatchEntry(wr, i);
        }
        wr.write("\n");//The end flag of the first target sequence.
        //cout << "Save the first target sequence match result complete." << endl;
    }

    private static int getHashValue(MatchEntry _me)
    {
        int result = 0;
        for (int i = 0; i < _me.getMisStr().length(); i++)
            result += _me.getMisStr().charAt(i) * 92083;
        result += _me.getPos() * 69061 + _me.getLength() * 51787;
        return result % seqBucketLen;
    }

    private static int getNextPrime( int number)
    {
        int cur = number + 1;
        boolean prime = false;
        while (!prime)
        {
            prime = true;
            for (int l = 2; l < sqrt(number) + 1; l++)
            {
                if (cur%l == 0)
                {
                    prime = false;
                    break;
                }
            }

            if (!prime) cur++;
        }
        return cur;
    }

    //1、hashvalue的计算公式，最好要做到一个字符不同，value就不同；length*7+pos*11+
//2、hashtable的长度如何设置；=》hashtable长度根据matchentry的个数来，取个数以下最近的素数
//3、loc的长度如何设置；=>loc的长度取me的个数；
    private static void matchResultHashConstruct(ArrayList<MatchEntry> _mr)
    {
        int hashValue1, hashValue2, hashValue;
        ArrayList<Integer> seqLoc = new ArrayList<>();
        int []seqBucket = new int[seqBucketLen];
        for (int i = 0; i < seqBucketLen; i++)
            seqBucket[i] = -1;
        //construct hash table
        hashValue1 = getHashValue(_mr.get(0));
        if (_mr.size() < 2) hashValue2 = 0;
        else hashValue2 = getHashValue(_mr.get(1));
        hashValue = abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < _mr.size()-1; i++)
        {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(_mr.get(i+1));
            hashValue = abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }

        //int[] intArr = seqLoc.stream().mapToInt(Integer::intValue).toArray();//巧妙地转换
        seqLoc_vec.add(seqLoc);
        seqBucket_vec.add(seqBucket);
        //seqLoc.clear();
    }

    private static boolean compareMatchEntry(MatchEntry ref, MatchEntry tar)//complete
    {
        return  ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr());
    }

    private static int getMatchLength(ArrayList <MatchEntry> ref_me, int ref_idx, ArrayList <MatchEntry> tar_me, int tar_idx)
    {
        int length = 0;
        while (ref_idx < ref_me.size() && tar_idx < tar_me.size() && compareMatchEntry(ref_me.get(ref_idx++), tar_me.get(tar_idx++)))
            length++;
        return length;
    }

    private static void codeSecondMatch(BufferedWriter wr, ArrayList<MatchEntry> _mr, int seqNum)throws IOException
    {
        int hashValue;
        int pre_seq_id=1;
        int max_pos=0, pre_pos=0, delta_pos, length, max_length, delta_length, seq_id=0, delta_seq_id;
        int id, pos, secondMatchTotalLength=0;
        int i;
        for (i = 0; i < _mr.size()-1; i++) {
            if(_mr.size()<2) hashValue = abs(getHashValue(_mr.get(i))) % seqBucketLen;
            else hashValue = abs(getHashValue(_mr.get(i)) + getHashValue(_mr.get(i+1))) % seqBucketLen;
            max_length = 0;
            for (int m = 0; m < min( seqNum-1, sec_seq_num ); m++) {
                //获得数组类型arraylist内容
                id = seqBucket_vec.get(m)[hashValue];
                if (id!=-1) {
                    //获得ArrayList类型的arraylist内容
                    for (pos = id; pos!=-1; pos = seqLoc_vec.get(m).get(pos)) {
                        length = getMatchLength(matchResult_vec.get(m), pos, _mr, i);
                        if (length > 1 && length > max_length) {
                            seq_id = m + 1;  //在seqBucket是第m个，但是在seqName里面其实是第m+1个
                            max_pos = pos;
                            max_length = length;
                        }
                    }
                }
            }
            if (max_length!=0)
            {
                delta_seq_id = seq_id - pre_seq_id;//delta encoding, 把seq_id变成0
                delta_length = max_length - 2;//delta encoding, 减小length的数值大小，因为最小替换长度是2
                delta_pos = max_pos - pre_pos;//delta encoding, 减小pos的数值大小
                pre_seq_id = seq_id;
                pre_pos = max_pos + max_length;
                secondMatchTotalLength += max_length;

                //firstly save mismatched matchentry
                if (!misMatchEntry.isEmpty())
                {
                    for (MatchEntry k:misMatchEntry) {
                        saveMatchEntry(wr,k);
                    }
                    //misMatchEntry.clear();   //只是清空vector中的元素，并不释放内存，如果要释放内存，需要用swap()函数
                }
                misMatchEntry = new ArrayList<>();

                //secondly save matched matchentry
                wr.write(delta_seq_id+" ");
                wr.write(delta_pos+" ");
                wr.write(delta_length+" ");
                wr.flush();
                //fprintf(fp, "%d %d %d\n", delta_seq_id, delta_pos, delta_length);
                i += max_length - 1;//移动i的位置
            }
            else
            {
                misMatchEntry.add(_mr.get(i));
            }
        }
        if (i == _mr.size()-1)  misMatchEntry.add(_mr.get(i));
        if (!misMatchEntry.isEmpty())
        {
            for (int j = 0; j < misMatchEntry.size(); j++)
                saveMatchEntry(wr, misMatchEntry.get(j));
            misMatchEntry.clear();
        }
        wr.write("\n");

        System.out.println("code second match complete. The second match length: "+secondMatchTotalLength);
    }

    private static void codeMatch(String filename)
    {
        File fp = new File(filename);
        ArrayList<MatchEntry> mr=new ArrayList<>();
        try {
            BufferedWriter wr =new BufferedWriter(new FileWriter(fp));
            sec_seq_num = (int) ceil(PERCENT * seqNumber / 100);//取整
            seqBucketLen = getNextPrime(VEC_SIZE);//大于VEC_SIZE的另一个素数
            //cout << "sec_seq_num: "<< sec_seq_num << endl;
            for (int i = 1; i < seqNumber; i++)
            {
                targetSequenceExtraction(seqName.get(i));
                saveOtherData(wr, seq_low_len, nCha_len, spe_cha_len);
                codeFirstMatch(seq_code, seq_code_len,mr);
                if (i <= sec_seq_num)
                {
                    matchResult_vec.add(mr);
                    matchResultHashConstruct(mr);
                }
                if (i == 1)
                    saveFirstMatchResult(wr, mr);
                else
                    codeSecondMatch(wr, mr, i);
                mr.clear();
            }

            wr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //rewind(fp);//move fp to the begin of the file
    }

    private static void extractFileName(char []srcFileName, char []destFileName)
    {
        int i=0, j=0;
        for (; i < srcFileName.length; i++) //get the characters between '/' and '.'
        {
            if (srcFileName[i] == '/')
            {
                j = 0;
            }
            else
            {
                if (srcFileName[i] == '.')
                    break;
                else
                    destFileName[j++] = srcFileName[i];
            }
        }
        //destFileName[j] = '\0';
    }

    private static void compress(String filename)//暂时不知道如何获取
    {
        readFile(filename);//得出seqNumber的值，initial需要用
        if (seqNumber > 1)
        {
            String[]a1=filename.split("\\.");
            filename=a1[0];
            initial();
            char []temp_filename=new char[filename.length()];
            String resultFilename1;
            String resultFilename2;
            //char cmd[]={};
            extractFileName(filename.toCharArray(), temp_filename);
            resultFilename1=String.valueOf(temp_filename)+".hrcm";
            resultFilename2=String.valueOf(temp_filename)+".desc";
            //sprintf(cmd, "./7za a %s.7z %s %s -m0=PPMd", temp_filename, resultFilename1, resultFilename2);
            referenceSequenceExtraction(seqName.get(0));
            kMerHashingConstruct();

            codeMatch(resultFilename1);

            int[] line = lineWidth_vec.stream().mapToInt(Integer::intValue).toArray();

            File fp = new File(resultFilename2);
            BufferedWriter wr ;
            try {
                wr = new BufferedWriter(new FileWriter(fp));
                runLengthCoding(wr, line,seqNumber - 1, 0);//save lineWidth data
                saveIdentifierData(wr);//save identifier data
                wr.close();
                System.out.println("compress complete");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else
            System.out.println("Error: There is not any to-be-compressed sequence, nothing to be done.\n");
    }

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();
        //boolean compressed = false, decompressed = false;
        String file="J:\\study120\\study\\chr1\\namechr1.txt";
        //mode = compress or decompress
        compress(file);
        seqLoc_vec=null;
        System.out.println("总耗费时间为" + (System.currentTimeMillis() - startTime) + "ms");
    }
}
