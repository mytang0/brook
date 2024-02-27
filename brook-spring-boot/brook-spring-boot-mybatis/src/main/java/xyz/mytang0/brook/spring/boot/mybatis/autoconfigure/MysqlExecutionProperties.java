package xyz.mytang0.brook.spring.boot.mybatis.autoconfigure;

import lombok.Data;

import static xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants.DEFAULT_FLOW_PENDING_TABLE_NAME;
import static xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants.DEFAULT_FLOW_TABLE_NAME;
import static xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants.DEFAULT_TASK_PENDING_TABLE_NAME;
import static xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants.DEFAULT_TASK_TABLE_NAME;

@Data
public class MysqlExecutionProperties {

    /**
     * The name of the table where flow instances are stored.
     */
    private String flowTableName = DEFAULT_FLOW_TABLE_NAME;

    /**
     * The name of the table where pending process instances are stored.
     */
    private String flowPendingTableName = DEFAULT_FLOW_PENDING_TABLE_NAME;

    /**
     * The name of the table where task instances are stored.
     */
    private String taskTableName = DEFAULT_TASK_TABLE_NAME;

    /**
     * The name of the table where pending task instances are stored.
     */
    private String taskPendingTableName = DEFAULT_TASK_PENDING_TABLE_NAME;
}
