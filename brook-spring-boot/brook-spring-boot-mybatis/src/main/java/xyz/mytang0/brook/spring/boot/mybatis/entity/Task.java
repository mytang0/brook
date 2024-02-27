package xyz.mytang0.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@TableName("${executionDaoTaskTableName}")
public class Task extends BasicEntity {

    private static final long serialVersionUID = -5353820216001575891L;

    private String flowId;

    private String taskId;

    private String taskName;

    private String jsonDef;

    private String jsonData;
}
