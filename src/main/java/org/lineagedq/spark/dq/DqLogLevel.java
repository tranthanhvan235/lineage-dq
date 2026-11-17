package org.lineagedq.spark.dq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Ep rieng package cua framework DQ ve muc INFO, bat ke job da goi setLogLevel("WARN").
 *
 * Vi sao can: pipeline goi {@code spark.sparkContext().setLogLevel("WARN")} ngay sau khi tao
 * session, va lenh do dat muc cho ROOT logger cua ca JVM. Hau qua do duoc that: moi dong
 * "=== [gate x] rows=... score=..." bi nuot sach, nen mot lan chay THANH CONG trong y het mot
 * lan khong chay gi - chi nhanh that bai (WARN/ERROR) moi hien ra. Mat luon truc thoi gian de
 * doi chieu pipeline voi agent.
 *
 * GOI BANG REFLECTION CO CHU Y: log4j-core nam trong image Spark chu khong phai dependency cua
 * framework. Compile thang vao se rang buoc framework voi mot ban log4j cu the; reflection +
 * catch Throwable bien "khong co log4j" thanh mot dong debug thay vi lam hong job - dung
 * nguyen tac cua ca framework.
 */
final class DqLogLevel {

    private static final Logger LOG = LoggerFactory.getLogger(DqLogLevel.class);
    private static final String PACKAGE = "org.lineagedq.spark.dq";

    private static boolean done;

    private DqLogLevel() {
    }

    /** Idempotent - goi nhieu lan khong sao. */
    static synchronized void ensureInfo() {
        if (done || !DqConfig.FORCE_INFO_LOG) return;
        done = true;
        try {
            Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            Object info = levelClass.getMethod("valueOf", String.class).invoke(null, "INFO");
            Method setLevel = configurator.getMethod("setLevel", String.class, levelClass);
            setLevel.invoke(null, PACKAGE, info);
            LOG.info("=== DqLogLevel: da ep {} ve INFO (job dat root = WARN)", PACKAGE);
        } catch (Throwable t) {
            LOG.debug("Khong ep duoc muc log cho {} ({}) - bo qua.", PACKAGE, t.toString());
        }
    }
}
