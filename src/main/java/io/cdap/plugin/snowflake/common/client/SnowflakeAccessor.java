/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.snowflake.common.client;

import com.google.common.base.Strings;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;
import io.cdap.plugin.snowflake.common.OAuthUtil;
import io.cdap.plugin.snowflake.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.snowflake.common.util.QueryUtil;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.http.impl.client.HttpClients;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * A class which accesses Snowflake API.
 */
public class SnowflakeAccessor {
  private static final int LIMIT_ROWS = 5;

  private final BaseSnowflakeConfig config;
  protected final SnowflakeBasicDataSource dataSource;

  public SnowflakeAccessor(BaseSnowflakeConfig config) {
    this.config = config;
    this.dataSource = new SnowflakeBasicDataSource();
    initDataSource(dataSource, config);
  }

  public void runSQL(String query) throws IOException {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement populateStmt = connection.prepareStatement(query);) {
      populateStmt.execute();
    } catch (SQLException e) {
      throw new IOException(e);
    }

  }

  /**
   * Returns field descriptors for specified import query.
   *
   * @return List of field descriptors.
   * @throws IOException thrown if there are any issue with the I/O operations.
   */
  public List<SnowflakeFieldDescriptor> describeQuery(String query) throws IOException {
    String importQuery = QueryUtil.limitQuery(query, LIMIT_ROWS);
    List<SnowflakeFieldDescriptor> fieldDescriptors = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(importQuery);
         ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String name = metaData.getColumnName(i);
        int type = metaData.getColumnType(i);
        boolean nullable = metaData.isNullable(i) == ResultSetMetaData.columnNullable;
        fieldDescriptors.add(new SnowflakeFieldDescriptor(name, type, nullable));
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
    return fieldDescriptors;
  }

  private void initDataSource(SnowflakeBasicDataSource dataSource, BaseSnowflakeConfig config) {
    dataSource.setDatabaseName(config.getDatabase());
    dataSource.setSchema(config.getSchemaName());
    dataSource.setUrl(String.format("jdbc:snowflake://%s.snowflakecomputing.com", config.getAccountName()));

    String warehouse = config.getWarehouse();
    if (!Strings.isNullOrEmpty(warehouse)) {
      dataSource.setWarehouse(warehouse);
    }

    String role = config.getRole();
    if (!Strings.isNullOrEmpty(role)) {
      dataSource.setRole(role);
    }

    if (config.getOauth2Enabled()) {
      String accessToken = OAuthUtil.getAccessTokenByRefreshToken(HttpClients.createDefault(), config);
      dataSource.setOauthToken(accessToken);
    } else if (config.getKeyPairEnabled()) {
      dataSource.setUser(config.getUsername());

      String privateKeyPath = writeTextToTmpFile(config.getPrivateKey());
      try {
        dataSource.setPrivateKeyFile(privateKeyPath, config.getPassphrase());
      } finally {
        new File(privateKeyPath).deleteOnExit();
      }
    } else {
      dataSource.setUser(config.getUsername());
      dataSource.setPassword(config.getPassword());
    }
    String connectionArguments = config.getConnectionArguments();
    if (!Strings.isNullOrEmpty(connectionArguments)) {
      addConnectionArguments(dataSource, connectionArguments);
    }
  }

  /**
   * Checks connection to the service by testing API endpoint, in case
   * of exception would be generated {@link ConnectionTimeoutException}
   */
  public void checkConnection() {
    try (Connection connection = dataSource.getConnection()) {
      connection.getMetaData();
    } catch (SQLException e) {
      throw new ConnectionTimeoutException("Cannot create Snowflake connection.", e);
    }
  }

  private PrivateKey getPrivateKey(BaseSnowflakeConfig config) {
    if (Strings.isNullOrEmpty(config.getPrivateKey())) {
      throw new IllegalArgumentException("Cannot access the private key.");
    }

    File file = new File(config.getPrivateKey());
    try (FileInputStream fileInputStream = new FileInputStream(file);
         DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {

      byte[] keyBytes = new byte[(int) file.length()];
      dataInputStream.readFully(keyBytes);

      String encrypted = new String(keyBytes);
      String passphrase = Strings.isNullOrEmpty(config.getPassphrase())
        ? ""
        : config.getPassphrase();
      encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
      encrypted = encrypted.replace("-----END ENCRYPTED PRIVATE KEY-----", "");
      EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
      PBEKeySpec keySpec = new PBEKeySpec(passphrase.toCharArray());
      SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
      PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(encodedKeySpec);
    } catch (IOException | NoSuchAlgorithmException | InvalidKeyException | InvalidKeySpecException e) {
      throw new IllegalArgumentException("Cannot access the private key.", e);
    }
  }

  // SnowflakeBasicDataSource doesn't provide access for additional properties.
  private void addConnectionArguments(SnowflakeBasicDataSource dataSource, String connectionArguments) {
    try {
      Class<? extends SnowflakeBasicDataSource> dataSourceClass = dataSource.getClass();
      Field propertiesField = dataSourceClass.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      Properties properties = (Properties) propertiesField.get(dataSource);
      for (KeyValue<String, String> argument : KeyValueListParser.DEFAULT.parse(connectionArguments)) {
        properties.setProperty(argument.getKey(), argument.getValue());
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalArgumentException(
        String.format("Cannot set connection arguments '%s'.", connectionArguments), e);
    }
  }

  private static String writeTextToTmpFile(String text) {
    try {
      File temp = File.createTempFile("cdap_key", ".tmp");
      temp.setReadable(true, true); // set readable only by owner

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
        bw.write(text);
      }
      return temp.getPath();
    } catch (IOException e) {
      throw new RuntimeException("Cannot write key to temporary file", e);
    }
  }
}
