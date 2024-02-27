package xyz.mytang0.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${executionDaoTaskPendingTableName}")
public class TaskPending extends BasicEntity {

    private static final long serialVersionUID = -7308711781793691705L;

    private String taskId;

    private String taskName;

    private String flowId;
}
