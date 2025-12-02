/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.dds;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author maochonxin.mcx
 * @since 2025-12-03
 */
public class DdsWeightSwitchHelper {

    private final String     consoleBaseUrl;
    private final String     user;
    private final String     token;
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT    = 30000;

    public DdsWeightSwitchHelper(String consoleBaseUrl, String user, String token) {
        this.consoleBaseUrl = consoleBaseUrl;
        this.user = user;
        this.token = token;
    }

    /**
     * switch read write weight
     * 
     * @param appName the application name
     * @param appDsName the application data source name
     * @param version the version
     * @param dbkeySet the new dbkey set configuration, format: group_00,dbkey1:R1W1,dbkey2:R10W10
     * @param esSwitch whether to enable read write separation
     * @return the switch result
     * @throws Exception if switch fails
     */
    public boolean switchWeight(String appName, String appDsName, String version, String dbkeySet,
                                boolean esSwitch) throws Exception {
        String groupClusterName = getGroupClusterName(appName, appDsName, version);
        return switchWeightByGroupCluster(groupClusterName, dbkeySet, esSwitch);
    }

    /**
     * switch read write weight by groupClusterName
     * 
     * @param groupClusterName the group cluster name
     * @param dbkeySet the dbkey set configuration
     * @param esSwitch whether to enable read write separation
     * @return the switch result
     * @throws Exception if switch fails
     */
    public boolean switchWeightByGroupCluster(String groupClusterName, String dbkeySet, 
                                            boolean esSwitch) throws Exception {
        String urlString = String.format("%s/api/ops/ALIPAY/dev/groupCluster/%s/switchWeightByPost?user=%s&token=%s",
            consoleBaseUrl, groupClusterName, user, token);

        JSONObject requestBody = new JSONObject();
        requestBody.put("dbkeySet", dbkeySet);
        requestBody.put("esSwitch", esSwitch);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            // write request body
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestBody.toJSONString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            String responseBody = readResponse(connection);
            
            if (responseCode != 200) {
                throw new RuntimeException("switch read write weight failed: " + responseCode + " " + connection.getResponseMessage());
            }
            
            WeightSwitchResponse response = JSON.parseObject(responseBody, WeightSwitchResponse.class);
            return response.getSuccess() != null && response.getSuccess();
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * get the groupClusterName corresponding to appDsName
     * 
     * @param appName the application name
     * @param appDsName the application data source name
     * @param version the version
     * @return the groupClusterName
     * @throws Exception if get failed
     */
    public String getGroupClusterName(String appName, String appDsName, String version)
                                                                                       throws Exception {
        MetaDataResponse response = getMetaData(appName, version, "RDB");

        if (response.getData() != null) {
            for (AppDsInfo appDsInfo : response.getData()) {
                if (appDsName.equals(appDsInfo.getAppDsName())) {
                    return appDsInfo.getGroupCluster().getGroupClusterName();
                }
            }
        }

        throw new IllegalArgumentException(String.format(
            "no groupClusterName found for appDsName=%s", appDsName));
    }

    /**
     * get all data source metadata information of the application
     * 
     * @param appName the application name
     * @param appVersion the application version
     * @param dataType the data type
     * @return the metadata information
     * @throws Exception if request fails
     */
    public MetaDataResponse getMetaData(String appName, String appVersion, String dataType)
                                                                                           throws Exception {
        String urlString = String
            .format(
                "%s/api/query/ALIPAY/dev/appDs/metaQuery?appName=%s&appVersion=%s&dataType=%s&user=%s&token=%s",
                consoleBaseUrl, appName, appVersion, dataType, user, token);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);

            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("get metadata failed: " + responseCode + " "
                                           + connection.getResponseMessage());
            }

            String responseBody = readResponse(connection);
            return JSON.parseObject(responseBody, MetaDataResponse.class);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * read HTTP response
     * 
     * @param connection HTTP connection
     * @return the response content
     * @throws Exception if read fails
     */
    private String readResponse(HttpURLConnection connection) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line).append("\n");
            }
            return response.toString().trim();
        }
    }

    /**
     * weight switch response
     */
    public static class WeightSwitchResponse {
        private Boolean success;
        private String  message;
        private Object  data;

        public Boolean getSuccess() {
            return success;
        }

        public void setSuccess(Boolean success) {
            this.success = success;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }
    }

    /**
     * metadata response
     */
    public static class MetaDataResponse {
        private java.util.List<AppDsInfo> data;
        private ExtInfo                   extInfo;
        private Boolean                   success;

        public java.util.List<AppDsInfo> getData() {
            return data;
        }

        public void setData(java.util.List<AppDsInfo> data) {
            this.data = data;
        }

        public ExtInfo getExtInfo() {
            return extInfo;
        }

        public void setExtInfo(ExtInfo extInfo) {
            this.extInfo = extInfo;
        }

        public Boolean getSuccess() {
            return success;
        }

        public void setSuccess(Boolean success) {
            this.success = success;
        }
    }

    /**
     * cluster group configuration
     */
    public static class GroupCluster {
        private String groupClusterName;

        public String getGroupClusterName() {
            return groupClusterName;
        }

        public void setGroupClusterName(String groupClusterName) {
            this.groupClusterName = groupClusterName;
        }
    }

    /**
     * extended information
     */
    public static class ExtInfo {
        private String hostName;
        private String ip;
        private String traceId;

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getTraceId() {
            return traceId;
        }

        public void setTraceId(String traceId) {
            this.traceId = traceId;
        }
    }

    /**
     * connection properties
     */
    public static class ConnProp {
        private String dbkeyName;

        public String getDbkeyName() {
            return dbkeyName;
        }

        public void setDbkeyName(String dbkeyName) {
            this.dbkeyName = dbkeyName;
        }
    }

    /**
     * application data source information
     */
    public static class AppDsInfo {
        private String                   appDsName;
        private String                   appName;
        private java.util.List<ConnProp> connProps;
        private GroupCluster             groupCluster;
        private String                   version;

        public String getAppDsName() {
            return appDsName;
        }

        public void setAppDsName(String appDsName) {
            this.appDsName = appDsName;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public java.util.List<ConnProp> getConnProps() {
            return connProps;
        }

        public void setConnProps(java.util.List<ConnProp> connProps) {
            this.connProps = connProps;
        }

        public GroupCluster getGroupCluster() {
            return groupCluster;
        }

        public void setGroupCluster(GroupCluster groupCluster) {
            this.groupCluster = groupCluster;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }
}
