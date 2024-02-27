package xyz.mytang0.brook.spring.boot.mybatis.mapper;


import xyz.mytang0.brook.spring.boot.mybatis.entity.Task;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface TaskMapper extends BaseMapper<Task> {

    List<Task> selectByFlowId(@Param("flowId") String flowId);

    Task selectByTaskName(@Param("flowId") String flowId,
                          @Param("taskName") String taskName);

    Task selectByTaskId(@Param("taskId") String taskId);

    int updateByTaskId(Task task);

    int deleteByTaskId(@Param("taskId") String taskId);

    int deleteByFlowId(@Param("flowId") String flowId);
}
