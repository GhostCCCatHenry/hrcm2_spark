package pojo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import pojo.compression_type;
import pojo.reference_type;

public class mykryo implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(reference_type.class, new FieldSerializer(kryo, reference_type.class));  //在Kryo序列化库中注册ref类
        kryo.register(compression_type.class, new FieldSerializer(kryo,compression_type.class));//在Kryo序列化库中注册target的类
        kryo.register(MatchEntry.class, new FieldSerializer(kryo,MatchEntry.class));
        //.register(tmp2.class, new FieldSerializer(kryo, tmp2.class));
    }
}
