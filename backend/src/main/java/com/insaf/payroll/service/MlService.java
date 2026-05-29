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

    public Object getAnomaliesByMinistry(String ministry) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(mlApiUrl + "/anomalies/by-ministry");
        if (ministry != null && !ministry.isBlank()) b.queryParam("ministry", ministry);
        return restTemplate.getForObject(b.toUriString(), Object.class);
    }

    public Object getAnomaliesByGrade(String ministry) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(mlApiUrl + "/anomalies/by-grade");
        if (ministry != null && !ministry.isBlank()) b.queryParam("ministry", ministry);
        return restTemplate.getForObject(b.toUriString(), Object.class);
    }

    public Object getAnomalyTemporalContext(int employeeSk, int yearNum, int monthNum) {
        String url = UriComponentsBuilder
                .fromHttpUrl(mlApiUrl + "/anomalies/temporal-context")
                .queryParam("employee_sk", employeeSk)
                .queryParam("year_num",    yearNum)
                .queryParam("month_num",   monthNum)
                .toUriString();
        return restTemplate.getForObject(url, Object.class);
    }

    public Object chat(Map<String, Object> body) {
        String url = mlApiUrl + "/chat";
        return restTemplate.postForObject(url, body, Object.class);
    }

    public Object getForecastDimensions(String ministry) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(mlApiUrl + "/forecast/dimensions");
        if (ministry != null && !ministry.isBlank()) b.queryParam("ministry", ministry);
        return restTemplate.getForObject(b.toUriString(), Object.class);
    }

    public Object getForecastHistorical(String ministry, String grade) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(mlApiUrl + "/forecast/historical");
        if (ministry != null && !ministry.isBlank()) b.queryParam("ministry", ministry);
        if (grade    != null && !grade.isBlank())    b.queryParam("grade",    grade);
        return restTemplate.getForObject(b.toUriString(), Object.class);
    }

    public Object getEmployeeForecast(String employeeId) {
        String url = UriComponentsBuilder
                .fromHttpUrl(mlApiUrl + "/forecast/employee")
                .queryParam("employee_id", employeeId)
                .toUriString();
        return restTemplate.getForObject(url, Object.class);
    }

    public Object getMlStatus() {
        try {
            return restTemplate.getForObject(mlApiUrl + "/", Object.class);
        } catch (Exception e) {
            return Map.of("status", "ML service unavailable", "error", e.getMessage());
        }
    }
}
