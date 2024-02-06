package org.mytang.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${queueTableName}")
public class QueueMessage extends BasicEntity {

    private static final long serialVersionUID = -6957342108762911238L;

    private String queueName;

    private String messageId;

    private int priority;

    private long deliveryTime;

    private String payload;
}
