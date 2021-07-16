/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pulsar;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.HashMap;
import java.util.Map;

/**
 * Common place to keep all pulsar consumer configs
 */
public class PulsarConsumerConfigs
{

  public static Map<String, Object> getConsumerProperties(Map<String, Object> consumerProperties)
  {
    final Map<String, Object> props = new HashMap<>();
    if (consumerProperties != null) {
      props.putAll(consumerProperties);
      if (!consumerProperties.containsKey("subscriptionName")) {
        props.put("subscriptionName", StringUtils.format("pulsar-supervisor-%s", IdUtils.getRandomId()));
      }
    }
    return props;
  }

  public static Map<String, Object> getClientConf(Map<String, Object> consumerProperties)
  {
    Map<String, Object> clientConf = new HashMap<>();
    if (consumerProperties.get("authPluginClassName") != null) {
      String authPluginClassName = (String) consumerProperties.get("authPluginClassName");
      clientConf.put("authPluginClassName", authPluginClassName);
    }
    if (consumerProperties.get("authParams") != null) {
      String authParams = (String) consumerProperties.get("authParams");
      clientConf.put("authParams", authParams);
    }
    if (consumerProperties.get("operationTimeoutMs") != null) {
      Long operationTimeoutMs = Long.valueOf(((String) consumerProperties.get("operationTimeoutMs")));
      clientConf.put("operationTimeoutMs", operationTimeoutMs);
    }
    if (consumerProperties.get("statsIntervalSeconds") != null) {
      Long statsIntervalSeconds = Long.valueOf(((String) consumerProperties.get("statsIntervalSeconds")));
      clientConf.put("statsIntervalSeconds", statsIntervalSeconds);
    }
    if (consumerProperties.get("numIoThreads") != null) {
      int numIoThreads = Integer.valueOf(((String) consumerProperties.get("numIoThreads")));
      clientConf.put("numIoThreads", numIoThreads);
    }
    if (consumerProperties.get("numListenerThreads") != null) {
      int numListenerThreads = Integer.valueOf(((String) consumerProperties.get("numListenerThreads")));
      clientConf.put("numListenerThreads", numListenerThreads);
    }
    if (consumerProperties.get("useTcpNoDelay") != null) {
      boolean useTcpNoDelay = Boolean.valueOf(((String) consumerProperties.get("useTcpNoDelay")));
      clientConf.put("useTcpNoDelay", useTcpNoDelay);
    }
    if (consumerProperties.get("useTls") != null) {
      boolean useTls = Boolean.valueOf(((String) consumerProperties.get("useTls")));
      clientConf.put("useTls", useTls);
    }
    if (consumerProperties.get("tlsTrustCertsFilePath") != null) {
      String tlsTrustCertsFilePath = (String) consumerProperties.get("tlsTrustCertsFilePath");
      clientConf.put("tlsTrustCertsFilePath", tlsTrustCertsFilePath);
    }
    if (consumerProperties.get("tlsAllowInsecureConnection") != null) {
      boolean tlsAllowInsecureConnection = Boolean.valueOf(((String) consumerProperties.get("tlsAllowInsecureConnection")));
      clientConf.put("tlsAllowInsecureConnection", tlsAllowInsecureConnection);
    }
    if (consumerProperties.get("tlsHostnameVerificationEnable") != null) {
      boolean tlsHostnameVerificationEnable = Boolean.valueOf(((String) consumerProperties.get("tlsHostnameVerificationEnable")));
      clientConf.put("tlsHostnameVerificationEnable", tlsHostnameVerificationEnable);
    }
    if (consumerProperties.get("concurrentLookupRequest") != null) {
      int concurrentLookupRequest = Integer.valueOf(((String) consumerProperties.get("concurrentLookupRequest")));
      clientConf.put("concurrentLookupRequest", concurrentLookupRequest);
    }
    if (consumerProperties.get("maxLookupRequest") != null) {
      int maxLookupRequest = Integer.valueOf(((String) consumerProperties.get("maxLookupRequest")));
      clientConf.put("maxLookupRequest", maxLookupRequest);
    }
    if (consumerProperties.get("maxNumberOfRejectedRequestPerConnection") != null) {
      int maxNumberOfRejectedRequestPerConnection = Integer.valueOf(((String) consumerProperties.get("maxNumberOfRejectedRequestPerConnection")));
      clientConf.put("maxNumberOfRejectedRequestPerConnection", maxNumberOfRejectedRequestPerConnection);
    }
    if (consumerProperties.get("keepAliveIntervalSeconds") != null) {
      int keepAliveIntervalSeconds = Integer.valueOf(((String) consumerProperties.get("keepAliveIntervalSeconds")));
      clientConf.put("keepAliveIntervalSeconds", keepAliveIntervalSeconds);
    }
    if (consumerProperties.get("connectionTimeoutMs") != null) {
      int connectionTimeoutMs = Integer.valueOf(((String) consumerProperties.get("connectionTimeoutMs")));
      clientConf.put("connectionTimeoutMs", connectionTimeoutMs);
    }
    if (consumerProperties.get("requestTimeoutMs") != null) {
      int requestTimeoutMs = Integer.valueOf(((String) consumerProperties.get("requestTimeoutMs")));
      clientConf.put("requestTimeoutMs", requestTimeoutMs);
    }
    if (consumerProperties.get("maxBackoffIntervalNanos") != null) {
      Long maxBackoffIntervalNanos = Long.valueOf(((String) consumerProperties.get("maxBackoffIntervalNanos")));
      clientConf.put("maxBackoffIntervalNanos", maxBackoffIntervalNanos);
    }
    return clientConf;
  }

  public static ClientConfigurationData createClientConf(Map<String, Object> consumerProperties)
  {
    ClientConfigurationData clientConf = new ClientConfigurationData();
    clientConf.setServiceUrl((String) consumerProperties.get("serviceUrl"));
    if (consumerProperties.get("authPluginClassName") != null) {
      String authPluginClassName = (String) consumerProperties.get("authPluginClassName");
      clientConf.setAuthPluginClassName(authPluginClassName);
    }
    if (consumerProperties.get("authParams") != null) {
      String authParams = (String) consumerProperties.get("authParams");
      clientConf.setAuthParams(authParams);
    }
    if (consumerProperties.get("operationTimeoutMs") != null) {
      Long operationTimeoutMs = Long.valueOf(((String) consumerProperties.get("operationTimeoutMs")));
      clientConf.setOperationTimeoutMs(operationTimeoutMs);
    }
    if (consumerProperties.get("statsIntervalSeconds") != null) {
      Long statsIntervalSeconds = Long.valueOf(((String) consumerProperties.get("statsIntervalSeconds")));
      clientConf.setStatsIntervalSeconds(statsIntervalSeconds);
    }
    if (consumerProperties.get("numIoThreads") != null) {
      int numIoThreads = Integer.valueOf(((String) consumerProperties.get("numIoThreads")));
      clientConf.setNumIoThreads(numIoThreads);
    }
    if (consumerProperties.get("numListenerThreads") != null) {
      int numListenerThreads = Integer.valueOf(((String) consumerProperties.get("numListenerThreads")));
      clientConf.setNumListenerThreads(numListenerThreads);
    }
    if (consumerProperties.get("useTcpNoDelay") != null) {
      boolean useTcpNoDelay = Boolean.valueOf(((String) consumerProperties.get("useTcpNoDelay")));
      clientConf.setUseTcpNoDelay(useTcpNoDelay);
    }
    if (consumerProperties.get("useTls") != null) {
      boolean useTls = Boolean.valueOf(((String) consumerProperties.get("useTls")));
      clientConf.setUseTls(useTls);
    }
    if (consumerProperties.get("tlsTrustCertsFilePath") != null) {
      String tlsTrustCertsFilePath = (String) consumerProperties.get("tlsTrustCertsFilePath");
      clientConf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
    }
    if (consumerProperties.get("tlsAllowInsecureConnection") != null) {
      boolean tlsAllowInsecureConnection = Boolean.valueOf(((String) consumerProperties.get("tlsAllowInsecureConnection")));
      clientConf.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
    }
    if (consumerProperties.get("tlsHostnameVerificationEnable") != null) {
      boolean tlsHostnameVerificationEnable = Boolean.valueOf(((String) consumerProperties.get("tlsHostnameVerificationEnable")));
      clientConf.setTlsHostnameVerificationEnable(tlsHostnameVerificationEnable);
    }
    if (consumerProperties.get("concurrentLookupRequest") != null) {
      int concurrentLookupRequest = Integer.valueOf(((String) consumerProperties.get("concurrentLookupRequest")));
      clientConf.setConcurrentLookupRequest(concurrentLookupRequest);
    }
    if (consumerProperties.get("maxLookupRequest") != null) {
      int maxLookupRequest = Integer.valueOf(((String) consumerProperties.get("maxLookupRequest")));
      clientConf.setMaxLookupRequest(maxLookupRequest);
    }
    if (consumerProperties.get("maxNumberOfRejectedRequestPerConnection") != null) {
      int maxNumberOfRejectedRequestPerConnection = Integer.valueOf(((String) consumerProperties.get("maxNumberOfRejectedRequestPerConnection")));
      clientConf.setMaxNumberOfRejectedRequestPerConnection(maxNumberOfRejectedRequestPerConnection);
    }
    if (consumerProperties.get("keepAliveIntervalSeconds") != null) {
      int keepAliveIntervalSeconds = Integer.valueOf(((String) consumerProperties.get("keepAliveIntervalSeconds")));
      clientConf.setKeepAliveIntervalSeconds(keepAliveIntervalSeconds);
    }
    if (consumerProperties.get("connectionTimeoutMs") != null) {
      int connectionTimeoutMs = Integer.valueOf(((String) consumerProperties.get("connectionTimeoutMs")));
      clientConf.setConnectionTimeoutMs(connectionTimeoutMs);
    }
    if (consumerProperties.get("requestTimeoutMs") != null) {
      int requestTimeoutMs = Integer.valueOf(((String) consumerProperties.get("requestTimeoutMs")));
      clientConf.setRequestTimeoutMs(requestTimeoutMs);
    }
    if (consumerProperties.get("maxBackoffIntervalNanos") != null) {
      Long maxBackoffIntervalNanos = Long.valueOf(((String) consumerProperties.get("maxBackoffIntervalNanos")));
      clientConf.setMaxBackoffIntervalNanos(maxBackoffIntervalNanos);
    }
    return clientConf;
  }

}
