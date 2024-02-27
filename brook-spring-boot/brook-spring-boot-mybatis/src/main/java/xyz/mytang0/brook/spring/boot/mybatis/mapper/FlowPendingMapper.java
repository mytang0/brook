package xyz.mytang0.brook.spring.boot.mybatis.mapper;

import xyz.mytang0.brook.spring.boot.mybatis.entity.FlowPending;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface FlowPendingMapper extends BaseMapper<FlowPending> {

    int deleteByFlowId(@Param("flowId") String flowId);

    List<String> selectFlowIds(@Param("flowName") String flowName);
}
