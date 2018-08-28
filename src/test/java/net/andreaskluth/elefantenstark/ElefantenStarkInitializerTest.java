package net.andreaskluth.elefantenstark;

import org.junit.jupiter.api.Test;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgres;

class ElefantenStarkInitializerTest {

    @Test
    void initializesSchema() throws Exception {
        withPostgres(connection -> new ElefantenStarkInitializer().init().accept(connection));
    }

}