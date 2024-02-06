package org.mytang.brook.spi.oss;

import org.mytang.brook.common.configuration.Validatable;
import org.mytang.brook.common.utils.StringUtils;
import lombok.Data;

import java.io.Serializable;

@Data
public class OSSStorage implements Validatable, Serializable {

    private static final long serialVersionUID = -7172071398124989603L;

    private String bucketName;

    private String key;

    private String downloadAddress;

    @Override
    public void validate() {
        if (StringUtils.isBlank(bucketName)) {
            throw new IllegalArgumentException("The bucketName is blank.");
        }

        if (StringUtils.isBlank(key)
                && StringUtils.isBlank(downloadAddress)) {
            throw new IllegalArgumentException("The key and downloadAddress are both blank.");
        }
    }
}
