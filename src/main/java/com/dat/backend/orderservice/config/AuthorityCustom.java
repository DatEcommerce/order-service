package com.dat.backend.orderservice.config;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AuthorityCustom implements Converter<Jwt, Collection<GrantedAuthority>> {

    private final String clientId;

    public AuthorityCustom(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        Map<String, Object> resourceAccess = jwt.getClaimAsMap("resource_access");
        if (resourceAccess == null) return List.of();

        Map<String, Object> clientRoles = (Map<String, Object>) resourceAccess.get(clientId);
        if (clientRoles == null || !clientRoles.containsKey("roles")) return List.of();

        List<String> roles = (List<String>) clientRoles.get("roles");

        return roles.stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .collect(Collectors.toList());
    }
}
