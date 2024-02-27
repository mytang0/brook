package xyz.mytang0.brook.spi.metadata;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;

@SPI(value = "file")
public interface MetadataService {

    void saveFlow(FlowDef flowDef);

    void updateFlow(FlowDef flowDef);

    void deleteFlow(String name);

    FlowDef getFlow(String name);
}
