package xyz.mytang0.brook.spring.boot.mybatis;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.spi.annotation.FlowSelectedSPI;
import xyz.mytang0.brook.spi.metadata.MetadataService;
import xyz.mytang0.brook.spring.boot.mybatis.entity.FlowDefinition;
import xyz.mytang0.brook.spring.boot.mybatis.mapper.FlowDefMapper;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@FlowSelectedSPI(name = "mysql")
@ConditionalOnProperty(name = "brook.metadata.mysql.enabled", havingValue = "true")
public class MysqlMetadataService implements MetadataService {

    private final Cache<String, Optional<FlowDef>> flowDefCache;

    private final FlowDefMapper flowDefMapper;

    public MysqlMetadataService(FlowDefMapper flowDefMapper) {
        this.flowDefMapper = flowDefMapper;
        this.flowDefCache = Caffeine
                .newBuilder()
                .maximumSize(512)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
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
        deleteFlow(name, null);
    }

    @Override
    public void deleteFlow(String name, Integer version) {
        flowDefMapper.delete(Wrappers
                .lambdaQuery(FlowDefinition.class)
                .eq(FlowDefinition::getName, name)
                .eq(Objects.nonNull(version),
                        FlowDefinition::getVersion,
                        version)
        );
    }

    @Override
    public FlowDef getFlow(String name) {
        return getFlow(name, null);
    }

    @Override
    public FlowDef getFlow(String name, Integer version) {
        String cacheKey =
                version != null
                        ? name + Delimiter.AT + version
                        : name;
        return Objects.requireNonNull(
                        flowDefCache.get(cacheKey,
                                __ -> getFlowOptional(name, version))
                )
                .orElse(null);
    }

    private Optional<FlowDef> getFlowOptional(String name, Integer version) {

        FlowDefinition store;

        if (version != null) {
            store = flowDefMapper.selectOne(Wrappers
                    .lambdaQuery(FlowDefinition.class)
                    .eq(FlowDefinition::getName, name)
                    .eq(FlowDefinition::getVersion, version));
        } else {
            store = flowDefMapper.selectOne(Wrappers
                    .lambdaQuery(FlowDefinition.class)
                    .eq(FlowDefinition::getName, name)
                    .orderByDesc(FlowDefinition::getId)
                    .last("LIMIT 1"));
        }

        if (store != null) {
            return Optional.of(JsonUtils.readValue(store.getJsonData(), FlowDef.class));
        }

        return Optional.empty();
    }
}
