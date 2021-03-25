package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

public class HotDictReloadThread implements Runnable {

    private static final Logger logger = Loggers.getLogger("ik-analyzer");

    @Override
    public void run() {
        while (true) {
            logger.info("[==============]reload hot dict from mysql......");
            Dictionary.getSingleton().loadMySQL();
        }
    }
}