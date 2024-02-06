package org.mytang.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${metadataTableName}")
public class FlowDefinition extends BasicEntity {

    private static final long serialVersionUID = -7418379349039306916L;

    private String name;

    private Integer version;

    private String description;

    private String jsonData;
}
