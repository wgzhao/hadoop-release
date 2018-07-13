/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Hashtable;

/**
 * Utilities class http query parameters.
 */
public class QueryParams {

    private Hashtable<String, String> params = new Hashtable<String, String>();
    private String apiVersion = null;
    private String separator = "";
    private String serializedString = null;

    public void add(String name, String value) {
        params.put(name, value);
        serializedString = null;
    }

    public void setApiVersion(String apiVersion) { this.apiVersion = apiVersion; serializedString = null; }

    public String serialize()  {
        if (serializedString == null) {
            StringBuilder sb = new StringBuilder();

            for (String name : params.keySet()) {
                try {
                    sb.append(separator);
                    sb.append(URLEncoder.encode(name, "UTF-8"));
                    sb.append('=');
                    sb.append(URLEncoder.encode(params.get(name), "UTF-8"));
                    separator = "&";
                } catch (UnsupportedEncodingException ex) {
                }
            }

            if (apiVersion != null) {
                sb.append(separator);
                sb.append("api-version=");
                sb.append(apiVersion);
                separator = "&";
            }
            serializedString = sb.toString();
        }
        return serializedString;
    }
}
