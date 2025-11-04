package ru.sinitsynme.sensors;

import ru.sinitsynme.util.AppProperties;

public class PrometheusSinkProperties {
    private String prometheusPushGatewayUrl;

    public PrometheusSinkProperties(AppProperties properties) {
        this.prometheusPushGatewayUrl = properties.getProperty("iot-monitoring-job.sink.prometheus-url");
    }

    public PrometheusSinkProperties(String prometheusPushGatewayUrl) {
        this.prometheusPushGatewayUrl = prometheusPushGatewayUrl;
    }

    public String getPrometheusPushGatewayUrl() {
        return prometheusPushGatewayUrl;
    }

    public void setPrometheusPushGatewayUrl(String prometheusPushGatewayUrl) {
        this.prometheusPushGatewayUrl = prometheusPushGatewayUrl;
    }
}
