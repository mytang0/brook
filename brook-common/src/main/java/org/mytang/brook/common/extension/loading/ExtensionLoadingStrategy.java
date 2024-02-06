package org.mytang.brook.common.extension.loading;


import org.mytang.brook.common.annotation.Order;

@Order
public class ExtensionLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/brook/extension/";
    }
}
