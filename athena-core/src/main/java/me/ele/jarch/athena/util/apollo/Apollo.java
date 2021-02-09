package me.ele.jarch.athena.util.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.util.AthenaConfig;
import me.ele.jarch.athena.util.deploy.dalgroupcfg.DalGroupConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.ele.jarch.athena.constant.Constants.APOLLO_DALGROUP_CFG_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_DALGROUP_META_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORGS_KEY;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORG_DEPLOY_PREFIX;
import static me.ele.jarch.athena.constant.Constants.HOSTNAME;

public class Apollo {
    private static final Logger logger = LoggerFactory.getLogger(Apollo.class);

    private static final Apollo INSTANCE = new Apollo();

    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private volatile Config config;

    public void init() {
        System.setProperty("apollo.meta", AthenaConfig.getInstance().getApolloMeta());
        System.setProperty("app.id", AthenaConfig.getInstance().getApolloAppid());
        System.setProperty("env", AthenaConfig.getInstance().getApolloEnv());
        System.setProperty("apollo.cluster", AthenaConfig.getInstance().getApolloCluster());

        String namespace = AthenaConfig.getInstance().getApolloNameSpace();
        config = ConfigService.getConfig(namespace);
    }

    public void loadConfig() {
        String orgsStr = config.getProperty(APOLLO_ORGS_KEY, "");
        config.addChangeListener(new AthenaConfigChangeListener());
        if (StringUtils.isEmpty(orgsStr)) {
            logger.error("Critical!! No org information found on Apollo, apollo meta:{}, appid:{}, env:{}, cluster{}," +
                            " namespace{}, orgKey{} ", AthenaConfig.getInstance().getApolloMeta(), AthenaConfig.getInstance().getApolloAppid(),
                    AthenaConfig.getInstance().getApolloEnv(), AthenaConfig.getInstance().getApolloCluster(),
                    AthenaConfig.getInstance().getApolloNameSpace(), APOLLO_ORGS_KEY);
            return;
        }
        String[] orgs = orgsStr.trim().split(",");
        Set<String> orgSet = Stream.of(orgs).map(String::trim).collect(Collectors.toSet());
        loadOrgs(orgSet);
    }

    public void loadOrgs(Set<String> orgSet) {
        Set<String> deployedOrg = new HashSet<>();
        for (String org : orgSet) {
            String orgKey = APOLLO_ORG_DEPLOY_PREFIX + org;
            String hosts = config.getProperty(orgKey, "");
            if (StringUtils.isNotBlank(hosts)) {
                if (Arrays.stream(hosts.split(",")).anyMatch(host -> host.trim().equals(HOSTNAME))) {
                    deployedOrg.add(org);
                }
            }
        }

        deployedOrg.stream().map(org -> config.getProperty(APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX + org, ""))
                .filter(StringUtils::isNotBlank)
                .flatMap(dalgroups -> Arrays.stream(dalgroups.split(",")))
                .forEach(this::loadDalgroup);
    }

    public void loadDalgroup(String dalGroup) {
        String dalGroupCfg = config.getProperty(APOLLO_DALGROUP_CFG_PREFIX + dalGroup, "");
        if (StringUtils.isNotBlank(dalGroupCfg)) {
            try {
                writerDalgroup2File(dalGroupCfg);
            } catch (Exception e) {
                logger.error("Failed to parse dalgroup config: {} ", dalGroupCfg, e);
            }
        } else {
            logger.warn("Got empty dalgroup config string for group {}", dalGroup);
        }
    }

    public void writerDalgroup2File(String value) throws IOException {
        DalGroupConfig config = objectMapper.readValue(value, DalGroupConfig.class);
        config.write2File();
    }

    public static void main(String[] args) throws Exception {
        AthenaConfig.getInstance().load();
        Apollo.getInstance().init();
        Apollo.getInstance().loadConfig();
        Thread.sleep(10000000000000L);
    }

    public static Apollo getInstance() {
        return INSTANCE;
    }

    public Config getConfig() {
        return config;
    }

    public Map<String, String> getDalgroupMeta(String dalgroup) {
        String metaStr = config.getProperty(APOLLO_DALGROUP_META_PREFIX + dalgroup, "");
        if (StringUtils.isNotBlank(metaStr)) {
            try {
                return objectMapper.readValue(metaStr, HashMap.class);
            } catch (Exception e) {
                logger.error("Failed to parse meta, dalgroup: {},  meta config {} ", dalgroup, metaStr, e);
            }
        }
        return Collections.emptyMap();
    }
}
