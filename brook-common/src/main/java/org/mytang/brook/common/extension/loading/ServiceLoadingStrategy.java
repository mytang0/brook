package org.mytang.brook.common.extension.loading;

import org.mytang.brook.common.annotation.Order;

@Order(Integer.MIN_VALUE)
public class ServiceLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/services/";
    }
}
