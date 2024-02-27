package xyz.mytang0.brook.metadata.http;

import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class HTTPMetadataServiceTest {

    private HTTPMetadataService metadataService;

    @Before
    public void setup() {
        this.metadataService = Mockito.mock(HTTPMetadataService.class);
        Mockito.when(metadataService.getFlow("test")).then(__ -> {
            FlowDef flowDef = new FlowDef();
            flowDef.setName("test");
            return flowDef;
        });
    }

    @After
    public void destroy() {
        metadataService.destroy();
    }

    @Test
    public void testGetFlow() {
        FlowDef flowDef = metadataService.getFlow("test");
        Assert.assertNotNull(flowDef);
        Assert.assertEquals("test", flowDef.getName());
        Assert.assertNull(metadataService.getFlow("null"));
    }
}