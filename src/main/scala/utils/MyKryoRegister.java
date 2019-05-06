package utils;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @program: Contact
 * @description: kryo序列化
 * @author: Mr.Xie
 * @create: 2018-12-11 16:41
 **/
public class MyKryoRegister implements KryoRegistrator{

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(MyKryoRegister.class);
    }
}
