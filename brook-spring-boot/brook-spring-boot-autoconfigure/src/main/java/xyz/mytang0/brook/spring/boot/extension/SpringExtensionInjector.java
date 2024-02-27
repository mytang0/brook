package xyz.mytang0.brook.spring.boot.extension;

import xyz.mytang0.brook.common.annotation.Order;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.injector.ExtensionInjector;
import xyz.mytang0.brook.common.utils.StringUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;

@Order(2)
public class SpringExtensionInjector implements ExtensionInjector {

    private ApplicationContext context;

    public void init(final ApplicationContext context) {
        this.context = context;
    }

    public static SpringExtensionInjector get() {
        return (SpringExtensionInjector) ExtensionDirector
                .getExtensionLoader(ExtensionInjector.class)
                .getExtension("spring");
    }

    @Override
    public <T> T getInstance(Class<T> type, String name) {
        if (context == null) {
            return null;
        }
        return getOptionalBean(context, name, type);
    }

    private <T> T getOptionalBean(final ListableBeanFactory beanFactory, final String name, final Class<T> type) {
        if (StringUtils.isEmpty(name)) {
            return getOptionalBeanByType(beanFactory, type);
        }
        if (beanFactory.containsBean(name)) {
            return beanFactory.getBean(name, type);
        }
        return null;
    }

    private <T> T getOptionalBeanByType(final ListableBeanFactory beanFactory, final Class<T> type) {
        String[] beanNamesForType = beanFactory.getBeanNamesForType(type, true, false);
        if (beanNamesForType.length > 1) {
            throw new IllegalStateException("Expect single but found " + beanNamesForType.length + " beans in spring context: " +
                    Arrays.toString(beanNamesForType));
        }
        return beanFactory.getBean(beanNamesForType[0], type);
    }
}
