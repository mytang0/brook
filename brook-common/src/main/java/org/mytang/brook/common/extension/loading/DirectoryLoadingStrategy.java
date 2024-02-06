package org.mytang.brook.common.extension.loading;

import org.mytang.brook.common.annotation.Order;

@Order(0)
public class DirectoryLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/brook/";
    }
}
