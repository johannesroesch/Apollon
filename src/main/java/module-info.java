module apollon {
    requires java.driver.core;
    requires java.driver.query.builder;
    requires slf4j.api;
    requires cassandra.all;
    requires org.apache.commons.lang3;
    requires com.google.common;
    requires io.netty.all;
    requires org.apache.commons.io;
    requires java.management;
    requires commons.collections;
    requires junit;
    requires metrics.core;

    exports io.github.johannesroesch.apollon.junit;
    exports io.github.johannesroesch.apollon.embedded;
    exports io.github.johannesroesch.apollon.exception;
}