package xyz.mytang0.brook.spring.boot.lock.redis;

import xyz.mytang0.brook.lock.redis.RedisLockService;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "brook.lock.redis.config")
@ConditionalOnProperty(name = "brook.lock.protocol", havingValue = "redis")
public class RedisLockAutoConfiguration extends Config {

    public ClusterServersConfig getClusterServersConfig() {
        return super.getClusterServersConfig();
    }

    @ConfigurationProperties("cluster-servers-config")
    public void setClusterServersConfig(ClusterServersConfig clusterServersConfig) {
        super.setClusterServersConfig(clusterServersConfig);
    }

    public SentinelServersConfig getSentinelServersConfig() {
        return super.getSentinelServersConfig();
    }

    @ConfigurationProperties("sentinel-servers-config")
    public void setSentinelServersConfig(SentinelServersConfig sentinelServersConfig) {
        super.setSentinelServersConfig(sentinelServersConfig);
    }

    public MasterSlaveServersConfig getMasterSlaveServersConfig() {
        return super.getMasterSlaveServersConfig();
    }

    @ConfigurationProperties("master-slave-servers-config")
    public void setMasterSlaveServersConfig(MasterSlaveServersConfig masterSlaveServersConfig) {
        super.setMasterSlaveServersConfig(masterSlaveServersConfig);
    }

    public SingleServerConfig getSingleServerConfig() {
        return super.getSingleServerConfig();
    }

    @ConfigurationProperties("single-server-config")
    public void setSingleServerConfig(SingleServerConfig singleServerConfig) {
        super.setSingleServerConfig(singleServerConfig);
    }

    @Bean
    public RedisLockService RedisLockService(final Config config) {
        if (getClusterServersConfig() == null
                && getSentinelServersConfig() == null
                && getMasterSlaveServersConfig() == null
                && getSingleServerConfig() == null) {
            throw new IllegalArgumentException(
                    "Please complete the Redis configuration (configured in Redisson format)."
            );
        }

        return new RedisLockService(config);
    }
}
