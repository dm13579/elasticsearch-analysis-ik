package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Set;

/**
 * User: dm
 * Date: 2021/03/22
 * Time: 13:08
 * Description: 数据库热更新词典线程
 */
public class DatabaseMonitor implements Runnable {
    private static final Logger LOGGER = Loggers.getLogger("ik-analyzer");
    private int periodMinutes;

    public DatabaseMonitor(int periodMinutes) {
        this.periodMinutes = periodMinutes;
        LOGGER.info("Constructed DatabaseMonitor");
    }

    @Override
    public void run() {
        try {
            DatabaseDictionary dbDict = DatabaseDictionary.getInstance();
            long lastUpdate = (System.currentTimeMillis() - periodMinutes * 60 * 1000) / 1000;

            Set<String> words = dbDict.fetchWords(lastUpdate, false, false);
            Set<String> stopwords = dbDict.fetchWords(lastUpdate, true, false);
            Set<String> deletedWords = dbDict.fetchWords(lastUpdate, false, true);
            Set<String> deletedStopwords = dbDict.fetchWords(lastUpdate, true, true);

            Dictionary dict = Dictionary.getSingleton();
            dict.addWords(words);
            dict.addStopwords(stopwords);
            dict.disableWords(deletedWords);
            dict.disableStopwords(deletedStopwords);
            // LOGGER.info("Updated dictionary from MySQL");
        } catch (Throwable t) {
            t.printStackTrace();
            LOGGER.error("Caught throwable in DatabaseMonitor. Message: " + t.getMessage());
            LOGGER.error("Stack trace:");
            for (StackTraceElement trace : t.getStackTrace()) {
                LOGGER.error(trace.toString());
            }
        }
    }
}