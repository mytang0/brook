package xyz.mytang0.brook.spi.oss;

import xyz.mytang0.brook.common.extension.SPI;

import java.io.InputStream;

@SPI
public interface OSSService {

    InputStream download(OSSStorage storage);
}
