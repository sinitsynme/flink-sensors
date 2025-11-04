package ru.sinitsynme.sensors;

import ru.sinitsynme.util.AppProperties;

public class AlertsSinkProperties {
    private String bootstrapServers;
    private String topic;

    public AlertsSinkProperties(AppProperties properties) {
        this.bootstrapServers = properties.getProperty("iot-monitoring-job.sink.bootstrap-servers");
        this.topic = properties.getProperty("iot-monitoring-job.sink.topic");
    }

    public AlertsSinkProperties(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
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
}
