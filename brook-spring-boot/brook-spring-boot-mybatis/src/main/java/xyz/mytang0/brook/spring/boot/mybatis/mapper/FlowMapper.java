package xyz.mytang0.brook.spring.boot.mybatis.mapper;

import xyz.mytang0.brook.spring.boot.mybatis.entity.Flow;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface FlowMapper extends BaseMapper<Flow> {

    Flow selectByFlowId(@Param("flowId") String flowId);

    Flow selectByCorrelationId(@Param("correlationId") String correlationId);

    int updateByFlowId(Flow flow);

    int deleteByFlowId(@Param("flowId") String flowId);
}
