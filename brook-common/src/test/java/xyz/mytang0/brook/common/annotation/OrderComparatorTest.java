package xyz.mytang0.brook.common.annotation;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class OrderComparatorTest {

    @Test
    public void sort() {
        List<Object> instances = new ArrayList<>();
        instances.add(InstanceA.instanceA);
        instances.add(InstanceB.instanceB);
        instances.add(InstanceC.instanceC);
        instances = instances.stream().sorted(OrderComparator.INSTANCE)
                .collect(Collectors.toList());
        Assert.assertEquals(InstanceB.instanceB, instances.get(0));
        Assert.assertEquals(InstanceA.instanceA, instances.get(1));
        Assert.assertEquals(InstanceC.instanceC, instances.get(2));
    }

    @Order(-1)
    static class InstanceA {

        static InstanceA instanceA = new InstanceA();
    }

    @Order(-5)
    static class InstanceB {

        static InstanceB instanceB = new InstanceB();
    }

    @Order(3)
    static class InstanceC {

        static InstanceC instanceC = new InstanceC();
    }
}