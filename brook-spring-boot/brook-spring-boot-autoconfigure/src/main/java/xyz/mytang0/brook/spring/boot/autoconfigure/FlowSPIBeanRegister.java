package xyz.mytang0.brook.spring.boot.autoconfigure;

import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.spi.annotation.FlowSPI;
import xyz.mytang0.brook.spi.annotation.FlowSelectedSPI;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;

@Slf4j
@Component
public class FlowSPIBeanRegister implements BeanPostProcessor {

    public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName)
            throws BeansException {

        Class<?> clazz = AopUtils.getTargetClass(bean);
        List<Class<?>> interfaces = ClassUtils.getAllInterfaces(clazz);
        if (CollectionUtils.isNotEmpty(interfaces)) {
            for (Class<?> inter : interfaces) {
                if (inter.isAnnotationPresent(SPI.class)) {
                    try {
                        String name = beanName;
                        FlowSPI flowSPI = clazz.getAnnotation(FlowSPI.class);
                        if (flowSPI != null) {
                            name = flowSPI.name();
                        } else {
                            FlowSelectedSPI flowSelectedSPI =
                                    clazz.getAnnotation(FlowSelectedSPI.class);
                            if (flowSelectedSPI != null) {
                                name = flowSelectedSPI.name();
                            }
                        }
                        ExtensionLoader<?> extensionLoader =
                                ExtensionDirector.getExtensionLoader(inter);
                        extensionLoader.addExtension(name, clazz, bean);
                    } catch (Throwable e) {
                        log.error("Spring SPI bean register to flow extension loader error", e);
                    }
                }
            }
        }
        return bean;
    }
}
