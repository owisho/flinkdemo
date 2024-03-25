package per.owisho.learn.flink.typeinfo;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.junit.Test;

public class TypeHintTest {

    @Test
    public void test(){
        TypeHint<Integer> integerTypeHint = new TypeHint<Integer>() {
            @Override
            public String toString() {
                return getClass()+"";
            }
        };

        System.out.println(integerTypeHint.toString());

    }

}
