package org.mytang.brook.spring.boot.mybatis.autoconfigure;

import lombok.Data;
import org.mytang.brook.spring.boot.mybatis.constants.TableNameConstants;

@Data
public class MysqlMetadataProperties {

    /**
     * The name of the table where metadata are stored.
     */
    private String tableName = TableNameConstants.DEFAULT_FLOW_DEF_TABLE_NAME;
}
