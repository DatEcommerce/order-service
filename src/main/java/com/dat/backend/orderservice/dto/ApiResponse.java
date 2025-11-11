package com.dat.backend.orderservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    private String message;
    private T data;
    private int statusCode;
    private LocalDateTime timestamp;

    public static<T> ApiResponse<T> success(int statusCode, T data) {
        return ApiResponse.<T>builder()
                .message("SUCCESS")
                .data(data)
                .statusCode(statusCode)
                .timestamp(LocalDateTime.now())
                .build();
    }

    public static<T> ApiResponse<T> success(T data) {
        return ApiResponse.<T>builder()
                .message("SUCCESS")
                .data(data)
                .statusCode(200)
                .timestamp(LocalDateTime.now())
                .build();
    }

    public static<T> ApiResponse<T> error(int statusCode, String message) {
        return ApiResponse.<T>builder()
                .message(message)
                .data(null)
                .statusCode(statusCode)
                .timestamp(LocalDateTime.now())
                .build();
    }
}
