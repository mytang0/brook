package xyz.mytang0.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${executionDaoFlowPendingTableName}")
public class FlowPending extends BasicEntity {

    private static final long serialVersionUID = -4330962044336817030L;

    private String flowId;

    private String flowName;
}
