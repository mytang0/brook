package xyz.mytang0.brook.demo.controller;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.core.metadata.MetadataFacade;
import xyz.mytang0.brook.core.metadata.MetadataProperties;
import xyz.mytang0.brook.spi.config.ConfiguratorFacade;

import javax.validation.Valid;

@RestController
@RequestMapping("/flow/metadata")
public class FlowMetadataController {

    private final MetadataFacade metadataFacade;

    public FlowMetadataController() {
        this.metadataFacade = new MetadataFacade(
                ConfiguratorFacade
                        .getConfig(MetadataProperties.class)
        );
    }

    @PostMapping
    public Boolean save(@RequestBody @Valid FlowDef flowDef) {
        metadataFacade.saveFlow(flowDef);
        return Boolean.TRUE;
    }

    @PutMapping
    public Boolean update(@RequestBody @Valid FlowDef newFlowDef) {
        metadataFacade.updateFlow(newFlowDef);
        return Boolean.TRUE;
    }

    @DeleteMapping
    public Boolean delete(@RequestParam("flowName") String flowName,
                          @RequestParam(value = "flowVersion", required = false) Integer flowVersion) {
        metadataFacade.deleteFlow(flowName, flowVersion);
        return Boolean.TRUE;
    }

    @GetMapping
    public FlowDef get(@RequestParam("flowName") String flowName,
                       @RequestParam(value = "flowVersion", required = false) Integer flowVersion) {
        return metadataFacade.getFlow(flowName, flowVersion);
    }
}
