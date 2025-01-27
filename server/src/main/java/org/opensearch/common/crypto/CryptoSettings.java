/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 *Dummy
 */
public class CryptoSettings {
    private static final Logger log = LogManager.getLogger(CryptoSettings.class);
    private final Map<EncryptionProvider,String > cryptoKeys;

    private static volatile CryptoSettings instance;

    /**
     *dummy
     * @param cryptoKeys
     */
    public CryptoSettings(Map<EncryptionProvider, String> cryptoKeys) {
        this.cryptoKeys = cryptoKeys;
        instance = this;
    }

    public static CryptoSettings getInstance() {
        return instance;
    }

    public static CryptoSettings initializeKMSKeys(
        EncryptionProvider encryptionProvider,
        String kmsKeyId) {
             Map<EncryptionProvider, String> cryptoKey = new HashMap<>();
             cryptoKey.put(encryptionProvider, kmsKeyId);
             instance = new CryptoSettings(cryptoKey);
             return instance;
        }

    /**
     *dummy
     * @param provider
     * @return
     */
    public String getCryptoKeys(EncryptionProvider provider) {
        if(instance.cryptoKeys.containsKey(provider)) {
            return instance.cryptoKeys.get(provider);
        }
        throw new IllegalArgumentException("Unknown encryption provider [" + provider + "]");
    }
}
