package xyz.mytang0.brook.core.metadata;

import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.spi.metadata.MetadataService;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class MetadataFacade implements MetadataService {

    private final MetadataProperties metadataProperties;

    private volatile MetadataService instance;

    public MetadataFacade(MetadataProperties metadataProperties) {
        this.metadataProperties = metadataProperties;
    }

    @Override
    public void saveFlow(FlowDef flowDef) {
        init();
        instance.saveFlow(flowDef);
    }

    @Override
    public void updateFlow(FlowDef flowDef) {
        init();
        instance.updateFlow(flowDef);
    }

    @Override
    public void deleteFlow(String name) {
        init();
        instance.deleteFlow(name);
    }

    @Override
    public FlowDef getFlow(String name) {
        init();
        return instance.getFlow(name);
    }

    private void init() {
        if (instance == null) {
            synchronized (this) {
                if (instance == null) {
                    instance = Optional.ofNullable(metadataProperties.getProtocol())
                            .filter(StringUtils::isNotBlank)
                            .map(protocol ->
                                    ExtensionDirector
                                            .getExtensionLoader(MetadataService.class)
                                            .getExtension(metadataProperties.getProtocol()))
                            .orElseGet(() ->
                                    ExtensionDirector
                                            .getExtensionLoader(MetadataService.class)
                                            .getDefaultExtension());
                }
            }
        }
    }
}
