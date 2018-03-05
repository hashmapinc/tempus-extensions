package com.hashmapinc.tempus.mqtt.config;

public class MqttSslContext {
    private String keyStoreFile;
    private String keyStorePassword;
    private String keyPassword;

    private String trustStoreFile;
    private String trustStorePassword;

    private final String JKS = "JKS";
    private final String TLS = "TLS";

    public MqttSslContext(String keyStoreFile, String keyPassword, String keyStorePassword, String trustStoreFile, String trustStorePassword) {
        this.keyStoreFile = keyStoreFile;
        this.keyPassword = keyPassword;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreFile = trustStoreFile;
        this.trustStorePassword = trustStorePassword;
    }

    public String getJKS() {
        return JKS;
    }

    public String getTLS() {
        return TLS;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public String getKeyStoreFile() {
        return keyStoreFile;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }
}
