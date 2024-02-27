package xyz.mytang0.brook.common.extension.loading;

import xyz.mytang0.brook.common.annotation.Order;

@Order(Integer.MIN_VALUE)
public class ServiceLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/services/";
    }
}
