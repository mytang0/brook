package org.mytang.brook.common.annotation;


import java.util.Comparator;

public final class OrderComparator<E> implements Comparator<E> {

    public static final OrderComparator<Object> INSTANCE
            = new OrderComparator<>();

    @Override
    public int compare(Object o1, Object o2) {
        Order order1 = o1.getClass().getAnnotation(Order.class);
        Order order2 = o2.getClass().getAnnotation(Order.class);
        int ov1 = order1 != null ? order1.value() : Integer.MAX_VALUE;
        int ov2 = order2 != null ? order2.value() : Integer.MAX_VALUE;
        return Integer.compare(ov1, ov2);
    }
}
