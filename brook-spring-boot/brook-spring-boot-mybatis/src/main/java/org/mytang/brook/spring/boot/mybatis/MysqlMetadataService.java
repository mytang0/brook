package org.mytang.brook.spring.boot.mybatis;

import org.mytang.brook.common.metadata.definition.FlowDef;
import org.mytang.brook.common.utils.JsonUtils;
import org.mytang.brook.spi.annotation.FlowSelectedSPI;
import org.mytang.brook.spi.metadata.MetadataService;
import org.mytang.brook.spring.boot.mybatis.entity.FlowDefinition;
import org.mytang.brook.spring.boot.mybatis.mapper.FlowDefMapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@FlowSelectedSPI(name = "mysql")
@ConditionalOnProperty(name = "brook.metadata.mysql.enabled", havingValue = "true")
public class MysqlMetadataService implements MetadataService {

    private final LoadingCache<String, Optional<FlowDef>> flowDefCache;

    private final FlowDefMapper flowDefMapper;

    public MysqlMetadataService(FlowDefMapper flowDefMapper) {
        this.flowDefMapper = flowDefMapper;
        this.flowDefCache = Caffeine
                .newBuilder()
                .maximumSize(512)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(this::getFlowOptional);
    }

    @Override
    public void saveFlow(FlowDef flowDef) {
        FlowDefinition store =
                new FlowDefinition();
        store.setName(flowDef.getName());
        store.setDescription(flowDef.getDescription());
        store.setJsonData(JsonUtils.toJsonString(flowDef));

        flowDefMapper.insert(store);
    }

    @Override
    public void updateFlow(FlowDef flowDef) {
        FlowDefinition newDef =
                new FlowDefinition();
        newDef.setDescription(flowDef.getDescription());
        newDef.setJsonData(JsonUtils.toJsonString(flowDef));

        flowDefMapper.update(
                newDef,
                Wrappers.lambdaQuery(FlowDefinition.class)
                        .eq(FlowDefinition::getName, flowDef.getName())
        );
    }

    @Override
    public void deleteFlow(String name) {
        flowDefMapper.delete(Wrappers
                .lambdaQuery(FlowDefinition.class)
                .eq(FlowDefinition::getName, name)
        );
    }

    @Override
    public FlowDef getFlow(String name) {
        return Objects.requireNonNull(flowDefCache.get(name)).orElse(null);
    }

    private Optional<FlowDef> getFlowOptional(String name) {
        FlowDefinition store =
                flowDefMapper.selectOne(Wrappers
                        .lambdaQuery(FlowDefinition.class)
                        .eq(FlowDefinition::getName, name));

        if (store != null) {
            return Optional.of(JsonUtils.readValue(store.getJsonData(), FlowDef.class));
        }

        return Optional.empty();
    }
}
