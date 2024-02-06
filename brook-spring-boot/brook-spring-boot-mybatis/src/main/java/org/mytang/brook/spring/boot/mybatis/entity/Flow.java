package org.mytang.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${executionDaoFlowTableName}")
public class Flow extends BasicEntity {

    private static final long serialVersionUID = -7918188414360297209L;

    private String flowId;

    private String flowName;

    private Integer flowVersion;

    private String correlationId;

    private String jsonData;
}
