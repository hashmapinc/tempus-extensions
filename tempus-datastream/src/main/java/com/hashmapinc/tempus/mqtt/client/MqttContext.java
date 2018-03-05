package com.hashmapinc.tempus.mqtt.client;

import com.hashmapinc.tempus.mqtt.config.MqttDefaultConfig;
import com.hashmapinc.tempus.mqtt.config.MqttProtocolType;
import com.hashmapinc.tempus.mqtt.config.MqttSslContext;
import com.hashmapinc.tempus.mqtt.config.MqttTbTopic;
import com.hashmapinc.tempus.mqtt.exception.MqttSslException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;

public class MqttContext {
    private String brokerUrl;
    private MqttConnectOptions options;
    private MqttTbTopic tbTopic;

    public MqttContext(String mqttBrokerIp, int mqttBrokerPort, MqttTbTopic topic, String username) {
        this.options = setMqttConnectOptions(username);
        this.tbTopic = topic;
        this.brokerUrl = getBrokerUrlStr(MqttProtocolType.TCP.getConnectionType(),mqttBrokerIp, mqttBrokerPort);
    }

    public MqttContext(String mqttBrokerIp, int mqttBrokerPort, MqttTbTopic topic, MqttSslContext mqttSslContext) throws MqttSslException {
        this.options = setMqttConnectOptions(mqttSslContext);
        this.tbTopic = topic;
        this.brokerUrl = getBrokerUrlStr(MqttProtocolType.SSL.getConnectionType(),mqttBrokerIp, mqttBrokerPort);
    }

    public MqttContext(String mqttBrokerIp, int mqttBrokerPort, MqttTbTopic topic) {
        MqttConnectOptions options = new MqttConnectOptions();
        this.options = setDefaultMqttOptions(options);
        this.tbTopic = topic;
        this.brokerUrl = getBrokerUrlStr(MqttProtocolType.TCP.getConnectionType(),mqttBrokerIp, mqttBrokerPort);
    }

    private MqttConnectOptions setMqttConnectOptions(String username) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword("".toCharArray());
        return setDefaultMqttOptions(options);
    }

    private MqttConnectOptions setMqttConnectOptions(MqttSslContext mqttSslContext) throws MqttSslException {
        SSLContext sslContext = getSslContext(mqttSslContext);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(sslContext.getSocketFactory());
        return setDefaultMqttOptions(options);
    }

    private SSLContext getSslContext(MqttSslContext mqttSslContext) throws MqttSslException {
        File ksFile = getStoreFile(mqttSslContext.getKeyStoreFile());
        File tsFile = getStoreFile(mqttSslContext.getTrustStoreFile());

        TrustManagerFactory tmf = setupTrustManagerFactory(mqttSslContext, tsFile);
        KeyManagerFactory kmf = setupKeyManagerFactory(mqttSslContext, ksFile);

        KeyManager[] km = kmf.getKeyManagers();
        TrustManager[] tm = tmf.getTrustManagers();

        return setupSslContext(km, tm, mqttSslContext.getTLS());
    }

    private File getStoreFile(String filename) {
            return new File(filename);
    }

    private TrustManagerFactory setupTrustManagerFactory(MqttSslContext mqttSslContext, File tsFile) throws MqttSslException {
        TrustManagerFactory tmf = createTrustManagerFactory();
        KeyStore trustStore = getStore(mqttSslContext.getJKS(), tsFile, mqttSslContext.getTrustStorePassword());
        return initTrustStore(tmf, trustStore);
    }

    private KeyManagerFactory setupKeyManagerFactory(MqttSslContext mqttSslContext, File ksFile) throws MqttSslException {
        KeyManagerFactory kmf = createKeyFactoryManager();
        KeyStore keyStore = getStore(mqttSslContext.getJKS(), ksFile, mqttSslContext.getKeyStorePassword());
        return initKeyStore(kmf, keyStore, mqttSslContext.getKeyPassword());
    }

    private SSLContext setupSslContext(KeyManager[] km, TrustManager[] tm, String tls) throws MqttSslException {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance(tls);
            sslContext.init(km, tm, null);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            throw new MqttSslException(ex);
        }
        return sslContext;
    }

    private TrustManagerFactory createTrustManagerFactory() throws MqttSslException {
        try {
            return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        } catch (NoSuchAlgorithmException ex) {
            throw new MqttSslException(ex);
        }
    }

    private KeyManagerFactory createKeyFactoryManager() throws MqttSslException {
        try {
            return KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } catch (NoSuchAlgorithmException ex) {
            throw new MqttSslException(ex);
        }
    }

    private TrustManagerFactory initTrustStore(TrustManagerFactory tmf, KeyStore trustStore) throws MqttSslException {
        try {
            tmf.init(trustStore);
        } catch (KeyStoreException ex) {
            throw new MqttSslException(ex);
        }
        return tmf;
    }

    private KeyManagerFactory initKeyStore(KeyManagerFactory kmf, KeyStore keyStore, String keyPassword) throws MqttSslException{
        try {
            kmf.init(keyStore, keyPassword.toCharArray());
        } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException ex) {
            throw new MqttSslException(ex);
        }
        return kmf;
    }

    private KeyStore getStore(String jks, File file, String password) throws MqttSslException {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(jks);
            keyStore.load(new FileInputStream(file), password.toCharArray());
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException ex) {
            throw new MqttSslException(ex);
        }
        return keyStore;
    }

    private MqttConnectOptions setDefaultMqttOptions(MqttConnectOptions options) {
        options.setCleanSession(MqttDefaultConfig.CLEAN_SESSION);
        options.setConnectionTimeout(MqttDefaultConfig.CONNECTION_TIMEOUT);
        options.setKeepAliveInterval(MqttDefaultConfig.KEEP_ALIVE_INTERVAL);
        options.setAutomaticReconnect(MqttDefaultConfig.AUTOMATIC_RECONNECT);
        options.setMaxInflight(MqttDefaultConfig.MAX_INFLIGHT);
        return options;
    }

    private String getBrokerUrlStr(String protocol, String ipAddress, int port) {
        return protocol + ipAddress + ":" + Integer.toString(port);
    }

    public MqttConnectOptions getMqttConnectOptions() {
        return options;
    }

    public MqttTbTopic getTbTopic() {
        return tbTopic;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }
}
