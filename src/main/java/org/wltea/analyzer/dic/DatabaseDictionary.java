package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugin.analysis.ik.AnalysisIkPlugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * User: dm
 * Date: 2021/03/22
 * Time: 13:08
 * Description: 数据库热更新词典
 */
public class DatabaseDictionary {

    private static final Logger LOGGER = Loggers.getLogger("ik-analyzer");
    private static final String DB_PROP_PATH = "db-ext-dict.properties";

    private static DatabaseDictionary instance;
    private Properties dbProperties;
    private Connection connection;

    private String getDictRoot() {
        return PathUtils.get(new File(
                AnalysisIkPlugin.class.getProtectionDomain().getCodeSource().getLocation().getPath()
        ).getParent(), "config").toAbsolutePath().toString();
    }

    private DatabaseDictionary() {
        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
            dbProperties = new Properties();
            LOGGER.info("文件目录："+PathUtils.get(getDictRoot()));
            dbProperties.load(new FileInputStream(PathUtils.get(getDictRoot(), DB_PROP_PATH).toFile()));
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            LOGGER.error("MySQL driver not found");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Error reading file " + DB_PROP_PATH);
        }
    }

    public static DatabaseDictionary getInstance() {
        if (instance == null) {
            synchronized (DatabaseDictionary.class) {
                if (instance == null) {
                    instance = new DatabaseDictionary();
                }
            }
        }
        return instance;
    }

    private void initConnection() {
        try {
            Driver driver = DriverManager.getDriver(dbProperties.getProperty("jdbc.url"));
            LOGGER.info("驱动名称"+driver.getClass().getName());
            connection = DriverManager.getConnection(
                    dbProperties.getProperty("jdbc.url"),
                    dbProperties.getProperty("jdbc.user"),
                    dbProperties.getProperty("jdbc.password")
            );

//             LOGGER.info("Created JDBC connnection");
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.error("Error creating JDBC connection: " + e.getMessage());
        }
    }

    private void closeConnection(ResultSet resultSet, PreparedStatement statement) {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
            if (statement != null) {
                statement.close();
                statement = null;
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
            // LOGGER.info("Closed JDBC connnection");
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.error("Error closing connection: " + e.getMessage());
        }
    }

    public Set<String> fetchWords(long lastUpdate, boolean isStopword, boolean isDeleted) {
        initConnection();
        Set<String> result = new HashSet<>();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            StringBuilder sql = new StringBuilder("select word from ");
            sql.append(dbProperties.getProperty("ext_dict.table.name"));
            sql.append(isDeleted ? " where is_deleted = 1 " : " where is_deleted = 0 ");
            sql.append(isStopword ? "and is_stopword = 1 " : "and is_stopword = 0 ");
            sql.append("and last_update >= ");
            sql.append(lastUpdate);
            LOGGER.info("sql执行语句，" + sql.toString());
            statement = connection.prepareStatement(sql.toString());
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String word = resultSet.getString("word");
                if (word != null && word.length() > 0) {
                    result.add(word);
                }
            }

            LOGGER.info("Executed query: " + sql.toString() + ", return count: " + result.size());
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.error("Error executing query of words: " + e.getMessage());
        } finally {
//            closeConnection(resultSet, statement);
        }

        return result;
    }
}
