package xyz.mytang0.brook.spi.annotation.selected;

import org.junit.Assert;
import org.junit.Test;
import xyz.mytang0.brook.common.extension.ExtensionLoader;

public class SelectedSPITest {

    @Test
    public void selected() {

        SelectedInterface selected =
                ExtensionLoader
                        .getExtensionLoader(SelectedInterface.class)
                        .getDefaultExtension();

        Assert.assertNotNull(selected);
    }
}
