package ru.sinitsynme.sensors;

import ru.sinitsynme.util.AppProperties;

public class SensorMetricsSourceProperties {
    private String bootstrapServers;
    private String topic;
    private String groupId;

    public SensorMetricsSourceProperties(AppProperties properties) {
        this.bootstrapServers = properties.getProperty("iot-monitoring-job.source.bootstrap-servers");
        this.topic = properties.getProperty("iot-monitoring-job.source.topic");
        this.groupId = properties.getProperty("iot-monitoring-job.source.group-id");
    }

    public SensorMetricsSourceProperties(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
