package org.mytang.brook.spring.boot.mybatis.mapper;


import org.mytang.brook.spring.boot.mybatis.entity.FlowDefinition;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface FlowDefMapper extends BaseMapper<FlowDefinition> {
}
