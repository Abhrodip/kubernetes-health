package com.gke.pod.health.service.impl;


import com.gke.pod.health.config.KafkaConfigNew;
import com.gke.pod.health.constants.HealthCheckConstants;
import com.gke.pod.health.entity.PodHealthResponse;
import com.gke.pod.health.service.PodHealthCountService;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PodHealthCountServiceImpl implements PodHealthCountService {

    private final AdminClient adminClient;

    private final DataSource dataSource;

    private final KafkaConfigNew kafkaConfigNew;


    @Value("${health.namespace.value}")
    private String nameSpaceValue;

    @Value("${health.applicationlist}")
    private String applications;

    @Value("${health.application.criteria}")
    private String criteria;


    @Value("#{${health.servicelist}}")
    private Map<String,String> serviceMap;


    @Value("#{${health.applist}}")
    private Map<String,String> applicationMap;


    public PodHealthCountServiceImpl(AdminClient adminClient, DataSource dataSource, KafkaConfigNew kafkaConfigNew1) {
        this.adminClient = adminClient;

        this.dataSource = dataSource;
        this.kafkaConfigNew = kafkaConfigNew1;
    }





    @Override
    public int getTotalPodCount(V1PodList v1PodList) {
//        log.info("Get TotalPodCount $$$"+ (int) v1PodList.getItems().stream().count());
        return (int) v1PodList.getItems().stream().count();
    }

    @Override
    public int getHealthyPodCount(V1PodList v1PodList) {
        return (int) v1PodList.getItems().stream().filter(pod->"Running".equalsIgnoreCase(pod.getStatus().getPhase())).count();
    }

    @Override
    public int getTotalHealthyPodCountUsingServiceName(V1PodList v1PodList,String serviceName) {
//        return (int) v1PodList.getItems().stream().filter(pod->pod.getMetadata().getLabels().containsKey("app")&& pod.getMetadata().getLabels().get("app").equalsIgnoreCase(serviceName)).count();
        return (int) v1PodList.getItems().stream()
                .filter(pod -> serviceName.equalsIgnoreCase(pod.getMetadata().getLabels() != null ? pod.getMetadata().getLabels().get("app") : null))
                .count();
    }

    @Override
    public int getHealthyPodCountUsingServiceName(V1PodList v1PodList,String serviceName) {

        List<V1Pod> podList=v1PodList.getItems().stream().filter(pod -> pod.getMetadata().getLabels().containsValue(serviceName)).collect(Collectors.toList());
//        for (V1Pod pod1 : podList) {
//            log.info("pod name new approach::::::" + pod1.getMetadata().getName());
//            for (V1ContainerStatus v1ContainerStatus : pod1.getStatus().getContainerStatuses()) {
//                log.info("containerName:::" + v1ContainerStatus.getName());
//                log.info("containerstatus::::" + v1ContainerStatus.getState());
//                if (v1ContainerStatus.getState().getTerminated()!=null || v1ContainerStatus.getState().getWaiting()!=null) {
//                    return 0;
//                }
//            }
//        }


        for (V1Pod pod1 : podList) {
            log.info("pod name ::::::" + pod1.getMetadata().getName());
            int containercount=0;
            // Check if containerStatuses is not null before iterating
            if (pod1.getStatus() != null && pod1.getStatus().getContainerStatuses() != null) {
//                log.info("containerSize:::"+pod1.getStatus().getContainerStatuses().size());
                for (V1ContainerStatus v1ContainerStatus : pod1.getStatus().getContainerStatuses()) {
                    log.info("containerName:::" + v1ContainerStatus.getName());
                    log.info("containerstatus::::" + v1ContainerStatus.getState());

                    // Check if container is in a Terminated or Waiting state
                    if (v1ContainerStatus.getState().getTerminated() != null || v1ContainerStatus.getState().getWaiting() != null) {
                        return 0;
                    }
                    else{
                        containercount++;
                    }
                }
                log.info("for the pod {} number of container running {}",pod1.getMetadata().getName(),containercount);
            } else {
                log.warn("Container statuses are not available for pod: " + pod1.getMetadata().getName());
            }
        }



//        return (int) v1PodList.getItems().stream().filter(pod -> pod.getMetadata().getLabels().containsKey("app") && pod.getMetadata().getLabels().get("app").equalsIgnoreCase(serviceName)
//                && pod.getStatus().getPhase().equalsIgnoreCase("Running")).count();
        return (int) v1PodList.getItems().stream()
                .filter(pod -> "Running".equalsIgnoreCase(pod.getStatus().getPhase())
                        && serviceName.equalsIgnoreCase(pod.getMetadata().getLabels().get("app")))
                .count();
    }
    @Override
    public PodHealthResponse getApplicationHealthStatus(int totalPodCount, int totalHealthyPodCount) {



        PodHealthResponse podHealthResponse=new PodHealthResponse();
        podHealthResponse.setTotalPodCount(totalPodCount);

        podHealthResponse.setTotalHealthyPodCount(totalHealthyPodCount);

        if((criteria != null || criteria != "") && criteria.equalsIgnoreCase(HealthCheckConstants.PERCENTAGE)){
            return statusCheckForPecentage(totalPodCount, totalHealthyPodCount, podHealthResponse);
        } else {
            return statusCheckForOther(totalPodCount, totalHealthyPodCount, podHealthResponse);
        }

    }

    private static PodHealthResponse statusCheckForOther(int totalPodCount, int totalHealthyPodCount, PodHealthResponse podHealthResponse) {
        log.info("totalPodCount==totalHealthyPodCount");
        return (totalPodCount==totalHealthyPodCount)?checkHealthy(podHealthResponse):checkNotHealthy(podHealthResponse);
    }

    private static PodHealthResponse statusCheckForPecentage(int totalPodCount, int totalHealthyPodCount, PodHealthResponse podHealthResponse) {
        log.info("totalHealthyPodCount < (0.7 * totalPodCount");
        return (totalHealthyPodCount < (0.7 * totalPodCount) || totalPodCount==0)?checkNotHealthy(podHealthResponse):checkHealthy(podHealthResponse);
    }



    private static  PodHealthResponse checkHealthy(PodHealthResponse podHealthResponse){

       podHealthResponse.setApplicationHealthStatus(HealthCheckConstants.HEALTHY);
       return podHealthResponse;
    }

    private static  PodHealthResponse checkNotHealthy(PodHealthResponse podHealthResponse){

        podHealthResponse.setApplicationHealthStatus(HealthCheckConstants.NOT_HEALTHY);
        return podHealthResponse;
    }




    @Override
    public V1PodList fetchPodList() throws IOException, ApiException {

        ApiClient apiClient= Config.defaultClient();
        CoreV1Api api=new CoreV1Api(apiClient);
        V1PodList podList= api.listNamespacedPod(nameSpaceValue).execute();
       // log.info("podList:::::"+podList);
        return podList;
    }

    @Override
    public int getServicesInNamespace() throws ApiException, IOException {
        // Fetch services from the specified namespace
        ApiClient apiClient = null;
        try {
            apiClient = Config.defaultClient();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        CoreV1Api api = new CoreV1Api(apiClient);
        V1ServiceList serviceList = api.listNamespacedService(
                nameSpaceValue // Allow watch bookmarks (optional)
        ).execute();

        log.info("serviceList:::::" + serviceList);



        V1PodList allPods = api.listNamespacedPod(nameSpaceValue).execute();

        String serviceName="il-audit-services";
        return (int) allPods.getItems().stream()
                .filter(pod -> serviceName.equalsIgnoreCase(pod.getMetadata().getLabels() != null ? pod.getMetadata().getLabels().get("app") : null))
                .count();

//        return (int) allPods.getItems().stream()
//                .filter(pod -> "Running".equalsIgnoreCase(pod.getStatus().getPhase())
//                        && serviceName.equalsIgnoreCase(pod.getMetadata().getLabels().get("app")))
//                .count();
    }








    @Override
    public PodHealthResponse fetchApplicationStatus(V1PodList podList) {
        PodHealthResponse podHealthResponse=null;
        Map<String,String> map=new HashMap<>();
        List<String> serviceList = Arrays.stream(applications.split(",")).toList();
        log.info("Get Total Running PodCount $$$"+ (int) podList.getItems().stream().filter(pod->"Running".equalsIgnoreCase(pod.getStatus().getPhase())).count());

        for (String serviceName : serviceList) {

            int totalHealthPodCountUsingServiceName = getTotalHealthyPodCountUsingServiceName(podList, serviceName);




            log.info("totalHealthPodCountUsingServiceName::::::" + totalHealthPodCountUsingServiceName + " serviceName:::: " + serviceName);
            int healthPodCountOnBasisOfService = getHealthyPodCountUsingServiceName(podList, serviceName);
            log.info("healthyPodCountOnBasisOfService::::::" + healthPodCountOnBasisOfService + " serviceName:::: " + serviceName);
            podHealthResponse = getApplicationHealthStatus(totalHealthPodCountUsingServiceName, healthPodCountOnBasisOfService);

            try {
                if (serviceMap.get(serviceName).equalsIgnoreCase(HealthCheckConstants.SERVICE_FLAG)) {
                    map.put(serviceName, podHealthResponse.getApplicationHealthStatus());
                }
            }
            catch (NullPointerException e)
            {
//                String status=HealthCheckConstants.NO_SERVICE;
                String status=checkApplicationStatusBasedOnServiceFlag(map);
                podHealthResponse.setApplicationHealthStatus(status);
                return podHealthResponse;
            }
        }
        String status=checkApplicationStatusBasedOnServiceFlag(map);
        podHealthResponse.setApplicationHealthStatus(status);

        return podHealthResponse;

    }
    @Override
    public Map<String, Object> fetchOverAllStatus(PodHealthResponse podHealthResponse, String health, String yugabyteDBStatus) {


        Map<String,String> kafkaStatus=new HashMap<>();
        kafkaStatus.put(HealthCheckConstants.STATUS,health);
        Map<String,String> kubernetesStatus=new HashMap<>();
        kubernetesStatus.put(HealthCheckConstants.STATUS,podHealthResponse.getApplicationHealthStatus());
        Map<String,String> yugabyeStatus=new HashMap<>();
        yugabyeStatus.put(HealthCheckConstants.STATUS,  yugabyteDBStatus);
        Map<String,Object> finalOutput=new HashMap<>();
        boolean flag=true;
        if(applicationMap.get(HealthCheckConstants.KAFKA).equalsIgnoreCase(HealthCheckConstants.SERVICE_FLAG))
        {
            finalOutput.put(HealthCheckConstants.KAFKA,kafkaStatus);
            if(kafkaStatus.containsValue(HealthCheckConstants.NOT_HEALTHY)){
                flag=false;
            }
        }
        if(applicationMap.get(HealthCheckConstants.YUGABYTE).equalsIgnoreCase(HealthCheckConstants.SERVICE_FLAG))
        {
            finalOutput.put(HealthCheckConstants.YUGABYTE,yugabyeStatus);
            if(yugabyeStatus.containsValue(HealthCheckConstants.NOT_HEALTHY)){
                flag=false;
            }
        }

        finalOutput.put(HealthCheckConstants.KUBERNETES,kubernetesStatus);

        if(kubernetesStatus.containsValue(HealthCheckConstants.NOT_HEALTHY)||kubernetesStatus.containsValue(HealthCheckConstants.NO_SERVICE)){
            flag=false;
        }

//        if(kafkaStatus.containsValue(HealthCheckConstants.NOT_HEALTHY) || kubernetesStatus.containsValue(HealthCheckConstants.NOT_HEALTHY) || yugabyeStatus.containsValue(HealthCheckConstants.NOT_HEALTHY)){
//            finalOutput.put(HealthCheckConstants.STATUS,HealthCheckConstants.NOT_HEALTHY);
//        }else{
//            finalOutput.put(HealthCheckConstants.STATUS,HealthCheckConstants.HEALTHY);
//        }
        if(flag==false)
        {
            finalOutput.put(HealthCheckConstants.STATUS,HealthCheckConstants.NOT_HEALTHY);
        }
        else{
            finalOutput.put(HealthCheckConstants.STATUS,HealthCheckConstants.HEALTHY);
        }

        return finalOutput;
    }

    @Override
    public String fetchYugabyteDBStatus() {

        try  {
            if (dataSource.getConnection().isValid(HealthCheckConstants.DATASOURCE_TIMEOUT)) {
                return HealthCheckConstants.HEALTHY;
            } else {

                return HealthCheckConstants.NOT_HEALTHY;
            }

        } catch (Exception e) {
            return HealthCheckConstants.NOT_HEALTHY;

        }
//        return "Up";
        }


       @Override
       public String getKafkaStatus() {

        try{
            DescribeClusterResult describeClusterRequest=adminClient.describeCluster();
            describeClusterRequest.nodes().get(HealthCheckConstants.TIMEOUT, TimeUnit.MILLISECONDS);
            return HealthCheckConstants.HEALTHY;
        }catch (Exception e){
            e.printStackTrace();
            log.info(":::::Kafka Timeout Exception::::::");
            return HealthCheckConstants.NOT_HEALTHY;
        }

    }

    private String checkApplicationStatusBasedOnServiceFlag(Map<String, String> map) {
        log.info("service Map::::::"+map);
        if(map.isEmpty())
        {
            log.info("service Map::::::null");
            return HealthCheckConstants.NO_SERVICE;
        }
        if(map!=null && map.containsValue(HealthCheckConstants.NOT_HEALTHY)){
                return HealthCheckConstants.NOT_HEALTHY;
            }
            return HealthCheckConstants.HEALTHY;
    }
}
