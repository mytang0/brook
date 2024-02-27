package xyz.mytang0.brook.common.extension.loading;


import xyz.mytang0.brook.common.annotation.Order;

@Order
public class ExtensionLoadingStrategy implements LoadingStrategy {

    @Override
    public String directory() {
        return "META-INF/brook/extension/";
    }
}
