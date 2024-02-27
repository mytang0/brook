package xyz.mytang0.brook.spring.boot.mybatis.mapper;

import xyz.mytang0.brook.spring.boot.mybatis.entity.TaskPending;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface TaskPendingMapper extends BaseMapper<TaskPending> {

    int deleteByTaskId(@Param("taskId") String taskId);

    int deleteByFlowId(@Param("flowId") String flowId);

    List<String> selectTaskIds(@Param("taskName") String taskName);
}
