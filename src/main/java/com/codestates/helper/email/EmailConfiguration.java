package com.codestates.helper.email;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class EmailConfiguration {
    @Bean
    public EmailSendable mockExceptionEmailSendable() {
        return new MockExceptionEmailSendable();
    }

    @Primary
    @Bean
    public EmailSendable simpleEmailSendable() {
        return new SimpleEmailSendable();
    }
}
