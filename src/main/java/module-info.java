module apollon {
    exports io.github.johannesroesch.apollon.junit;
    exports io.github.johannesroesch.apollon.embedded;
    exports io.github.johannesroesch.apollon.exception;
    requires junit;
    requires cassandra.all;
    requires commons.collections;
    requires org.apache.commons.lang3;
    requires org.apache.commons.io;
    requires java.driver.core;
    requires java.driver.query.builder;
    requires slf4j.api;
    requires com.google.common;
    requires io.netty.all;
    requires java.management;
}