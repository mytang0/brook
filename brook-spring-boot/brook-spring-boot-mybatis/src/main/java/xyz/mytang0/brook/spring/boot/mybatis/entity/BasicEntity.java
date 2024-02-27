package xyz.mytang0.brook.spring.boot.mybatis.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class BasicEntity implements Serializable {

    private static final long serialVersionUID = 6487088492954863430L;

    @TableId(type = IdType.AUTO)
    private Long id;

    private LocalDateTime rAddTime;

    private LocalDateTime rModifiedTime;

    private Boolean isDelete = false;
}
