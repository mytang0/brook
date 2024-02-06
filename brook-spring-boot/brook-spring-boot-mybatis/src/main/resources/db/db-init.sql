DROP TABLE `flow_def`;
CREATE TABLE `flow_def`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name`            varchar(255) COLLATE utf8mb4_bin NOT NULL COMMENT '名称',
    `version`         int(11) NOT NULL DEFAULT '0' COMMENT '版本',
    `description`     varchar(512) COLLATE utf8mb4_bin          DEFAULT NULL COMMENT '描述',
    `json_data`       text COLLATE utf8mb4_bin COMMENT '内容',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name_version` (`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='流程定义表';


DROP TABLE `flow`;
CREATE TABLE `flow`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `flow_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '流程实例ID',
    `flow_name`       varchar(255) COLLATE utf8mb4_bin NOT NULL COMMENT '流程定义名称',
    `flow_version`    int(11) NOT NULL DEFAULT '0' COMMENT '流程定义版本',
    `correlation_id`  varchar(64) COLLATE utf8mb4_bin           DEFAULT '0' COMMENT '相关ID',
    `json_data`       text COLLATE utf8mb4_bin COMMENT '流程实例内容',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_flow_id` (`flow_id`),
    KEY               `idx_correlation_id` (`correlation_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='流程表';


DROP TABLE `flow_pending`;
CREATE TABLE `flow_pending`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `flow_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '流程实例ID',
    `flow_name`       varchar(255) COLLATE utf8mb4_bin NOT NULL COMMENT '流程名称',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_flow_id` (`flow_id`),
    KEY               `idx_flow_name` (`flow_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='流程执行表';


DROP TABLE `task`;
CREATE TABLE `task`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `task_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '任务实例ID',
    `task_name`       varchar(255) COLLATE utf8mb4_bin NOT NULL COMMENT '任务名称',
    `flow_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '流程实例ID',
    `json_def`       text COLLATE utf8mb4_bin COMMENT '任务定义',
    `json_data`       text COLLATE utf8mb4_bin COMMENT '任务实例内容',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_task_id` (`task_id`),
    KEY               `idx_flow_id` (`flow_id`),
    KEY               `idx_task_name` (`task_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='任务表';


DROP TABLE `task_pending`;
CREATE TABLE `task_pending`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `task_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '任务实例ID',
    `task_name`       varchar(255) COLLATE utf8mb4_bin NOT NULL COMMENT '任务名称',
    `flow_id`         varchar(64) COLLATE utf8mb4_bin  NOT NULL COMMENT '流程实例ID',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_task_id` (`task_id`),
    KEY               `idx_flow_id` (`flow_id`),
    KEY               `idx_task_name` (`task_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='任务执行表';


DROP TABLE `queue`;
CREATE TABLE `queue`
(
    `id`              bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `queue_name`      varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '队列名',
    `message_id`      varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '消息ID',
    `priority`        int(10) unsigned NOT NULL DEFAULT '0' COMMENT '优先级',
    `delivery_time`   bigint(20) unsigned NOT NULL COMMENT '消息投递时间',
    `popped`          tinyint(1) DEFAULT '0' COMMENT '是否弹出 0：否；1：是',
    `payload`         text COLLATE utf8mb4_bin COMMENT '消息负载',
    `is_delete`       tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除 0：否；1：是',
    `r_add_time`      datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `r_modified_time` datetime                         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_queue_name_message_id` (`queue_name`,`message_id`),
    KEY               `idx_queue_name_delivery_time` (`queue_name`,`delivery_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='队列表';