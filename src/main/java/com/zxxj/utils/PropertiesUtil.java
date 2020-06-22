package com.zxxj.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
	private final static Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);

	private static Properties properties = new Properties();

	static {
		try {
            // load resources config
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("config.properties"));
			LOG.info("properties file loaded");
            String jarPath = PropertiesUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            System.out.println(jarPath);

            // load local config override resources config
            LOG.info("jar path {}", jarPath);
            int lastSlashIndex = jarPath.lastIndexOf("/");
            if (lastSlashIndex > 0) {
                String configFilePath = jarPath.substring(0, lastSlashIndex);
                File file = new File(configFilePath + "/conf/config.properties");
                System.out.println(file);
                if (file.exists()) {
                    properties.load(new FileReader(file));
                    LOG.info("properties file loaded"+configFilePath + "/conf/config.properties");
                } else {
                    LOG.warn("{} not exist", file.getPath());
                }
            } else {
                LOG.error("unknown jar path {}", jarPath);
            }
		} catch (Exception e) {
			LOG.error("load config.properties error.", e);
		}
	}

	public static void configure(String fileName) {
        LOG.info("config.properties file path {}", fileName);
        try {
            properties.load(new FileReader(fileName));
        } catch (IOException e) {
            LOG.error("unknown file path {}", fileName, e);
        }
    }
	
	public static String getString(String key) {
		return properties.getProperty(key);
	}

	public static int getInt(String key) {
		return Integer.parseInt(getString(key));
	}
}