/*
 * Copyright 2017 vefthym.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package minoaner.utils;

import com.esotericsoftware.kryo.Kryo;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.serializer.KryoRegistrator;

/**
 *
 * @author vefthym
 */
public class MyKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Integer.class);
        kryo.register(Integer[].class);
        kryo.register(Class.class/*, new ClassSerializer()*/); 
        kryo.register(Object.class);
        kryo.register(Object[].class);
        //kryo.register(VIntWritable.class);
        //kryo.register(VIntWritable[].class);
        //kryo.register(VIntArrayWritable.class);
        try {
            kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"));
            kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$2"));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyKryoRegistrator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
