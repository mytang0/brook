package org.mytang.brook.spi.metadata;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.metadata.definition.FlowDef;

@SPI(value = "file")
public interface MetadataService {

    void saveFlow(FlowDef flowDef);

    void updateFlow(FlowDef flowDef);

    void deleteFlow(String name);

    FlowDef getFlow(String name);
}
