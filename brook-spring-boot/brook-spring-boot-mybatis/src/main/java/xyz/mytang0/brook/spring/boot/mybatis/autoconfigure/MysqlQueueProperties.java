package xyz.mytang0.brook.spring.boot.mybatis.autoconfigure;

import lombok.Data;

import static xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants.DEFAULT_QUEUE_TABLE_NAME;

@Data
public class MysqlQueueProperties {

    /**
     * The name of the table where queue messages are stored.
     */
    private String tableName = DEFAULT_QUEUE_TABLE_NAME;
}
