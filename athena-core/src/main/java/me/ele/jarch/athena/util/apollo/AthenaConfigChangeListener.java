package me.ele.jarch.athena.util.apollo;

import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.ele.jarch.athena.netty.AthenaServer;
import me.ele.jarch.athena.scheduler.DBChannelDispatcher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static me.ele.jarch.athena.constant.Constants.APOLLO_DALGROUP_CFG_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_DALGROUP_META_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_GLOBAL_META_KEY;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORGS_KEY;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX;
import static me.ele.jarch.athena.constant.Constants.APOLLO_ORG_DEPLOY_PREFIX;
import static me.ele.jarch.athena.constant.Constants.HOSTNAME;

/**
 * 目前通过一个listener监听所有的配置变化， 未来改成基于不同的key传如不同的listener
 */
public class AthenaConfigChangeListener implements ConfigChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(AthenaConfigChangeListener.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    @Override
    public void onChange(ConfigChangeEvent changeEvent) {
        Set<String> changedKeys = changeEvent.changedKeys();
        changedKeys.forEach(key -> update(key, changeEvent.getChange(key)));
    }

    private void update(String key, ConfigChange change) {
        if (key.equals(APOLLO_ORGS_KEY)) {
            updateOrgs(key, change);
            return;
        }
        if (key.startsWith(APOLLO_ORG_DEPLOY_PREFIX)) {
            updateOrgDeployment(key, change);
            return;
        }
        if (key.startsWith(APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX)) {
            updateOrgDalGroupMapping(key, change);
            return;
        }
        if (key.startsWith(APOLLO_DALGROUP_CFG_PREFIX)) {
            updateDalgroupCfgChange(key, change);
            return;
        }
        if (key.startsWith(APOLLO_DALGROUP_META_PREFIX)) {
            updateDalgroupMetaChange(key, change);
            return;
        }
        if (key.equals(APOLLO_GLOBAL_META_KEY)) {
            updateGlobalMetaChange(key, change);
            return;
        }
        logger.warn("Got unknown  changes {}", change);
    }

    private void updateOrgs(String key, ConfigChange change) {
        switch (change.getChangeType()) {
            case ADDED:
                Set<String> addedOrgs = Arrays.stream(change.getNewValue().split(",")).map(String::trim).collect(Collectors.toSet());
                Apollo.getInstance().loadOrgs(addedOrgs);
                break;

            case MODIFIED:
                List<String> currentOrgs = Arrays.stream(change.getNewValue().split(",")).map(String::trim)
                        .distinct().collect(Collectors.toList());
                
                List<String> oldOrgs = Arrays.stream(change.getOldValue().split(",")).map(String::trim)
                        .distinct().collect(Collectors.toList());
                
                Set<String> newOrgs = currentOrgs.stream().filter(o -> !oldOrgs.contains(o)).collect(Collectors.toSet());
                Apollo.getInstance().loadOrgs(newOrgs);
                
                oldOrgs.stream().filter(o -> !currentOrgs.contains(o)).forEach(this::offlineOrg);
                break;

            case DELETED:
                Arrays.stream(change.getOldValue().split(",")).map(String::trim)
                        .distinct().forEach(this::offlineOrg);
                break;
        }
    }

    private void updateOrgDeployment(String key, ConfigChange change) {
        switch (change.getChangeType()) {
            case ADDED:
                boolean isDeployed = Arrays.stream(change.getNewValue().split(",")).map(String::trim).anyMatch(h -> h.equals(HOSTNAME));
                if (isDeployed) {
                    String org = key.replace(APOLLO_ORG_DEPLOY_PREFIX, "");
                    Apollo.getInstance().loadOrgs(Collections.singleton(org));
                }
                break;
                
            case MODIFIED:
                boolean isDeployedNow = Arrays.stream(change.getNewValue().split(",")).map(String::trim).anyMatch(h -> h.equals(HOSTNAME));
                boolean isDeployedBefore = Arrays.stream(change.getOldValue().split(",")).map(String::trim).anyMatch(h -> h.equals(HOSTNAME));
                String org = key.replace(APOLLO_ORG_DEPLOY_PREFIX, "");
                if (isDeployedNow && !isDeployedBefore) {
                    Apollo.getInstance().loadOrgs(Collections.singleton(org));
                } else if (!isDeployedNow && isDeployedBefore) {
                    offlineOrg(org);
                }
                break;
                
            case DELETED:
                offlineOrg(key.replace(APOLLO_ORG_DEPLOY_PREFIX, ""));
                break;
        }
    }

    private void updateOrgDalGroupMapping(String key, ConfigChange change) {
        switch (change.getChangeType()) {
            case ADDED:
                Set<String> addedOrgs = Arrays.stream(change.getNewValue().split(",")).map(String::trim).collect(Collectors.toSet());
                Apollo.getInstance().loadOrgs(addedOrgs);
                break;

            case MODIFIED:
                if (isOrgDeployed(key.replace(APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX, ""))) {
                    List<String> currentDalGroups = Arrays.stream(change.getNewValue().split(",")).map(String::trim)
                            .distinct().collect(Collectors.toList());
                    List<String> oldDalgroups = Arrays.stream(change.getOldValue().split(",")).map(String::trim)
                            .distinct().collect(Collectors.toList());
                    currentDalGroups.stream().filter(o -> !oldDalgroups.contains(o)).forEach(Apollo.getInstance()::loadDalgroup);
                    oldDalgroups.stream().filter(o -> !currentDalGroups.contains(o)).forEach(dalgroup ->
                            DBChannelDispatcher.getHolders().entrySet().stream().filter(e -> e.getKey().equals(dalgroup)).findAny()
                                    .ifPresent(e -> e.getValue().getDalGroupConfig().deleteFile()));
                }
                break;

            case DELETED:
                Arrays.stream(change.getOldValue().split(",")).map(String::trim)
                        .distinct().forEach(this::offlineOrg);
                break;
        }
    }

    private void updateDalgroupCfgChange(String key, ConfigChange change) {
        String dalgroup = key.replace(APOLLO_DALGROUP_CFG_PREFIX, "");
        if (!DBChannelDispatcher.getHolders().containsKey(dalgroup)) {
            return;
        }
        switch (change.getChangeType()) {
            case ADDED:
            case MODIFIED:
                if (StringUtils.isNotBlank(change.getNewValue())) {
                    try {
                        Apollo.getInstance().writerDalgroup2File(change.getNewValue());
                    } catch (Exception e) {
                        logger.error("Failed to parse dalgroup config: {} ", key, e);
                    }
                }
                break;
            case DELETED:
                DBChannelDispatcher.getHolders().entrySet().stream().filter(e -> e.getKey().equals(dalgroup)).findAny()
                        .ifPresent(e -> e.getValue().getDalGroupConfig().deleteFile());
                break;
        }
    }

    private void updateDalgroupMetaChange(String key, ConfigChange change) {
        String dalgroup = key.replace(APOLLO_DALGROUP_META_PREFIX, "");
        if (!DBChannelDispatcher.getHolders().containsKey(dalgroup)) {
            return;
        }
        switch (change.getChangeType()) {
            case ADDED:
            case MODIFIED:
                if (StringUtils.isNotBlank(change.getNewValue())) {
                    try {
                        Map<String, String> metas = objectMapper.readValue(change.getNewValue(), HashMap.class);
                        DBChannelDispatcher dispatcher = DBChannelDispatcher.getHolders().get(dalgroup);
                        if (Objects.nonNull(dispatcher)) {
                            metas.forEach((k, v) -> dispatcher.getZKCache().setZkCfg(k, v));
                        } else {
                            logger.warn("Dalgroup {} is not deployed!", dalgroup);
                        }
                    } catch (IOException e) {
                        logger.error("Failed to parse meta, dalgroup: {},  meta config {} ", dalgroup, change.getNewValue(), e);
                    }
                }
                break;
            case DELETED:
                break;
        }
    }

    private void updateGlobalMetaChange(String key, ConfigChange change) {
        switch (change.getChangeType()) {
            case ADDED:
            case MODIFIED:
                if (StringUtils.isNotBlank(change.getNewValue())) {
                    try {
                        Map<String, String> metas = objectMapper.readValue(change.getNewValue(), HashMap.class);
                        metas.forEach(AthenaServer.globalZKCache::setZkCfg);
                    } catch (IOException e) {
                        logger.error("Failed to parse global meta,  meta config {} ", change.getNewValue(), e);
                    }
                }
                break;
            case DELETED:
                break;
        }
    }

    private void offlineOrg(String org) {
        List<String> dalgroups = getDalGroupInOrg(org);
        dalgroups.forEach(dalGroup -> {
            DBChannelDispatcher.getHolders().entrySet().stream()
                    .filter(e -> e.getKey().equals(dalGroup)).findAny()
                    .ifPresent(e -> e.getValue().getDalGroupConfig().deleteFile());
        });
    }

    private List<String> getDalGroupInOrg(String org) {
        String metaStr = Apollo.getInstance().getConfig().getProperty(APOLLO_ORG_DALGROUP_MAPPING_KEY_PREFIX + org, "");
        if (StringUtils.isNotBlank(metaStr)) {
            return Arrays.stream(metaStr.split(",")).map(String::trim).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private boolean isOrgDeployed(String orgKey) {
        String org = orgKey.replace(APOLLO_ORG_DEPLOY_PREFIX, "");
        String deployedHosts = Apollo.getInstance().getConfig().getProperty(APOLLO_ORG_DEPLOY_PREFIX + org, "");
        return Arrays.stream(deployedHosts.split(",")).map(String::trim).anyMatch(h -> h.equals(HOSTNAME));
    }
}
