package xyz.mytang0.brook.spring.boot.mybatis.mapper;


import xyz.mytang0.brook.spring.boot.mybatis.entity.QueueMessage;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface QueueMapper extends BaseMapper<QueueMessage> {

    int inserts(@Param("list") List<QueueMessage> list);

    long recent(@Param("queueName") String queueName);

    int update(QueueMessage queueMessage);

    int delete(@Param("queueName") String queueName, @Param("messageId") String messageId);

    int deletes(@Param("queueName") String queueName, @Param("messageIds") List<String> messageIds);

    List<QueueMessage> poll(@Param("queueName") String queueName,
                            @Param("deliveryTime") Long deliveryTime,
                            @Param("maxCount") Integer maxCount);

    int popped(@Param("queueName") String queueName, @Param("messageIds") List<String> messageIds);

    int unacked(@Param("queueName") String queueName, @Param("timePoint") Long timePoint);
}
