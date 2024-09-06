package com.gke.pod.health.controller;

import com.gke.pod.health.entity.PodHealthResponse;
import com.gke.pod.health.service.PodHealthCountService;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/podhealth")
@Slf4j
public class PodHealthMonitorController {

    @Autowired
    private PodHealthCountService podHealthCountService;

    private PodHealthResponse podHealthResponse = null;

    private final DataSource dataSource;

    public PodHealthMonitorController(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    @GetMapping(value = "/getHealthOfApplication")
    public ResponseEntity<?> getApplicationHealthStatus() {
        try {
            V1PodList podList = podHealthCountService.fetchPodList();
            podHealthResponse = podHealthCountService.fetchApplicationStatus(podList);
            String kafkaStatus=podHealthCountService.getKafkaStatus();
            String yugabyteStatus=podHealthCountService.fetchYugabyteDBStatus();



            Map<String,Object> finalStatus= podHealthCountService.fetchOverAllStatus(podHealthResponse,kafkaStatus,yugabyteStatus);
            return ResponseEntity.ok(finalStatus);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @GetMapping("/services")
    public int getServices() throws ApiException, IOException {
        return podHealthCountService.getServicesInNamespace();
    }
}
