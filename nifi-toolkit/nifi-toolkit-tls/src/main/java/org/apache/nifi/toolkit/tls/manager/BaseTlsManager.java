/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.toolkit.tls.manager;

import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.manager.writer.ConfigurationWriter;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.PasswordUtil;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

public class BaseTlsManager {
    public static final String PKCS_12 = "PKCS12";
    private final TlsConfig tlsConfig;
    private final PasswordUtil passwordUtil;
    private final InputStreamFactory inputStreamFactory;
    private final KeyStore keyStore;
    private final List<ConfigurationWriter<TlsConfig>> configurationWriters;
    private boolean sameKeyAndKeyStorePassword = false;

    public BaseTlsManager(TlsConfig tlsConfig) throws GeneralSecurityException, IOException {
        this(tlsConfig, new PasswordUtil(), FileInputStream::new);
    }

    public BaseTlsManager(TlsConfig tlsConfig, PasswordUtil passwordUtil, InputStreamFactory inputStreamFactory) throws GeneralSecurityException, IOException {
        this.tlsConfig = tlsConfig;
        this.passwordUtil = passwordUtil;
        this.inputStreamFactory = inputStreamFactory;
        this.keyStore = loadKeystore(tlsConfig.getKeyStore(), tlsConfig.getKeyStoreType(), getKeyStorePassword());
        this.configurationWriters = new ArrayList<>();
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public KeyStore.Entry getEntry(String alias) throws UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException {
        String keyPassword = getKeyPassword();
        return keyStore.getEntry(alias, new KeyStore.PasswordProtection(keyPassword == null ? null : keyPassword.toCharArray()));
    }

    public KeyStore.Entry addPrivateKeyToKeyStore(KeyPair keyPair, String alias, Certificate... certificates) throws GeneralSecurityException, IOException {
        return addPrivateKeyToKeyStore(keyStore, keyPair, alias, getKeyPassword(), certificates);
    }

    private KeyStore.Entry addPrivateKeyToKeyStore(KeyStore keyStore, KeyPair keyPair, String alias, String passphrase, Certificate... certificates) throws GeneralSecurityException, IOException {
        keyStore.setKeyEntry(alias, keyPair.getPrivate(), passphrase == null ? null : passphrase.toCharArray(), certificates);
        return getEntry(alias);
    }

    public void setSameKeyAndKeyStorePassword(boolean sameKeyAndKeyStorePassword) {
        this.sameKeyAndKeyStorePassword = sameKeyAndKeyStorePassword;
    }

    private String getKeyPassword() {
        if (keyStore.getType().equalsIgnoreCase(PKCS_12)) {
            tlsConfig.setKeyPassword(null);
            return null;
        } else {
            String result = tlsConfig.getKeyPassword();
            if (StringUtils.isEmpty(result)) {
                if (sameKeyAndKeyStorePassword) {
                    result = getKeyStorePassword();
                } else {
                    result = passwordUtil.generatePassword();
                }
                tlsConfig.setKeyPassword(result);
            }
            return result;
        }
    }

    private String getKeyStorePassword() {
        String result = tlsConfig.getKeyStorePassword();
        if (StringUtils.isEmpty(result)) {
            result = passwordUtil.generatePassword();
            tlsConfig.setKeyStorePassword(result);
        }
        return result;
    }

    protected KeyStore loadKeystore(String keyStore, String keyStoreType, String keyStorePassword) throws GeneralSecurityException, IOException {
        KeyStore result;
        if (PKCS_12.equals(keyStoreType)) {
            result = KeyStore.getInstance(keyStoreType, BouncyCastleProvider.PROVIDER_NAME);
        } else {
            result = KeyStore.getInstance(keyStoreType);
        }
        File file = new File(keyStore);
        if (file.exists()) {
            try {
                result.load(inputStreamFactory.create(file), keyStorePassword.toCharArray());
                return result;
            } catch (Exception e) {
                result = KeyStore.getInstance(keyStoreType);
            }
        }
        result.load(null, null);
        return result;
    }

    public void write(OutputStreamFactory outputStreamFactory) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        String keyStorePassword = getKeyStorePassword();

        try (OutputStream outputStream = outputStreamFactory.create(new File(tlsConfig.getKeyStore()))) {
            keyStore.store(outputStream, keyStorePassword.toCharArray());
        }

        for (ConfigurationWriter<TlsConfig> configurationWriter : configurationWriters) {
            configurationWriter.write(tlsConfig);
        }
    }

    public PasswordUtil getPasswordUtil() {
        return passwordUtil;
    }

    public void addConfigurationWriter(ConfigurationWriter<TlsConfig> configurationWriter) {
        configurationWriters.add(configurationWriter);
    }

    public TlsConfig getTlsConfig() {
        return tlsConfig;
    }
}
