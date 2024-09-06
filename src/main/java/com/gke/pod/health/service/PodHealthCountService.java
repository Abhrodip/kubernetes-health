package com.gke.pod.health.service;

import com.gke.pod.health.entity.PodHealthResponse;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1PodList;

import java.io.IOException;
import java.util.Map;


public interface PodHealthCountService {

    public int getTotalPodCount(V1PodList v1PodList);

    public int getHealthyPodCount(V1PodList v1PodList);

    int getTotalHealthyPodCountUsingServiceName(V1PodList v1PodList,String serviceName);

    int getHealthyPodCountUsingServiceName(V1PodList v1PodList,String serviceName);

    public PodHealthResponse getApplicationHealthStatus(int totalPodCount, int totalHealthyPodCount);

    V1PodList fetchPodList() throws IOException, ApiException;

    int getServicesInNamespace() throws ApiException, IOException;

    PodHealthResponse fetchApplicationStatus(V1PodList podList);

  //  Health getKafkaHealth();

    Map<String,Object> fetchOverAllStatus(PodHealthResponse podHealthResponse,String health,String map);

    String fetchYugabyteDBStatus();
//
//
    String getKafkaStatus();



}
