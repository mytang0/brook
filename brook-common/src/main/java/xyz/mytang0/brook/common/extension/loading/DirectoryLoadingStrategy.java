package xyz.mytang0.brook.common.extension.loading;

import xyz.mytang0.brook.common.annotation.Order;

@Order(0)
public class DirectoryLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/brook/";
    }
}
