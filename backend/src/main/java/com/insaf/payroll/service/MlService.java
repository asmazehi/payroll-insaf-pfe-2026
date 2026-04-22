package com.insaf.payroll.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

/**
 * Proxies ML requests to the Python FastAPI service (localhost:8000).
 * Spring Boot handles auth + business logic; Python handles model inference.
 */
@Service
public class MlService {

    @Value("${app.ml.api-url}")
    private String mlApiUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    public Object getForecast(int n) {
        String url = UriComponentsBuilder
                .fromHttpUrl(mlApiUrl + "/forecast")
                .queryParam("n", n)
                .toUriString();
        return restTemplate.getForObject(url, Object.class);
    }

    public Object getAnomalies(int limit, String ministry, Integer year) {
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromHttpUrl(mlApiUrl + "/anomalies")
                .queryParam("limit", limit);
        if (ministry != null) builder.queryParam("ministry", ministry);
        if (year     != null) builder.queryParam("year",     year);
        return restTemplate.getForObject(builder.toUriString(), Object.class);
    }

    public Object chat(String question) {
        String url = mlApiUrl + "/chat";
        return restTemplate.postForObject(url, Map.of("question", question), Object.class);
    }

    public Object getMlStatus() {
        try {
            return restTemplate.getForObject(mlApiUrl + "/", Object.class);
        } catch (Exception e) {
            return Map.of("status", "ML service unavailable", "error", e.getMessage());
        }
    }
}
