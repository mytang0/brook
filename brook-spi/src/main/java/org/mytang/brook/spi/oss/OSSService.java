package org.mytang.brook.spi.oss;

import org.mytang.brook.common.extension.SPI;

import java.io.InputStream;

@SPI
public interface OSSService {

    InputStream download(OSSStorage storage);
}
