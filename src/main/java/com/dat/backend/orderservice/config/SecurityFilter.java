package com.dat.backend.orderservice.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityFilter {

    @Value("${keycloak.client-id}")
    private String clientId;

    private final String[] PUBLIC_URLS = {
            "api/v1/orders/test",
            "/api/v1/orders/**",
    };

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers(PUBLIC_URLS).permitAll()
                .anyRequest().authenticated() // Require authentication for all other requests
            )
                .oauth2ResourceServer(conf -> conf.jwt(
                        jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter()
                )))
                .csrf(org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer::disable); // Disable CSRF for simplicity, enable in production

        return http.build();
    }

    @Bean
    public JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtAuthenticationConverter jwtAuthenticationConverter = new JwtAuthenticationConverter();
        jwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(jwt -> {
            AuthorityCustom authorityCustom = new AuthorityCustom(clientId);
            return authorityCustom.convert(jwt);
        });
        return jwtAuthenticationConverter;
    }
}
