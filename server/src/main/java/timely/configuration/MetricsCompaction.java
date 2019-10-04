package timely.configuration;

import java.util.HashMap;

public class MetricsCompaction {

    private boolean autoConfigure = false;
    private HashMap<String, String> optionsDefault = new HashMap<>();
    private HashMap<String, String> optionsMetric = new HashMap<>();

    public HashMap<String, String> getDefaultStrategyOptions() {
        return optionsDefault;
    }

    public boolean getAutoConfigure() {
        return autoConfigure;
    }

    public void setAutoConfigure(boolean autoConfigure) {
        this.autoConfigure = autoConfigure;
    }

    public void setDefaultStrategyOptions(HashMap<String, String> options) {
        this.optionsDefault = options;
    }

    public HashMap<String, String> getMetricStrategyOptions() {
        return optionsMetric;
    }

    public void setMetricStrategyOptions(HashMap<String, String> options) {
        this.optionsMetric = options;
    }
}
