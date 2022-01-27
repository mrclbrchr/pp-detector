package com.ppspringdetector.ppdetector;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONString;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Configuration
@EnableScheduling
@SpringBootApplication
public class PpDetectorApplication {
    static Consul consulClient = Consul.builder().build();
    static HealthClient healthClient = consulClient.healthClient();
    static List<ServiceHealth> serviceHealthList = new ArrayList<>();
    static List processorList = new ArrayList<>();
    static List sinkList = new ArrayList<Map>();
    static List streamList = new ArrayList<>();
    static List setList = new ArrayList<Map>();

    public static void main(String[] args) {
        SpringApplication.run(PpDetectorApplication.class, args);
        checkForHealthServices();
    }

    public static void checkForHealthServices(){
        serviceHealthList = healthClient.getHealthyServiceInstances("pp-library").getResponse();
        for(int i = 0; i < serviceHealthList.size(); i++) {
            Service service = serviceHealthList.get(i).getService();
            addService(service);
        }
    }

    @Scheduled(fixedRate = 3000)
    public void refreshHealthServices() {
        processorList.clear();
        sinkList.clear();
        setList.clear();
        streamList.clear();
        serviceHealthList = healthClient.getHealthyServiceInstances("pp-library").getResponse();
        for(int i = 0; i < serviceHealthList.size(); i++) {
            Service service = serviceHealthList.get(i).getService();
            addService(service);
        }
        printLists();
    }

    public static void addService(Service service){
        JSONObject serviceMeta = new JSONObject(service.getMeta());
        String serviceType = serviceMeta.getString("type");
        if(serviceType.contains("processor")){
            processorList.add(service);
        } else if(serviceType.contains("sink")){
            sinkList.add(service);
        } else if(serviceType.contains("stream")){
            streamList.add(service);
        } else if(serviceType.contains("set")){
            setList.add(service);
        }
    }

    public static void printLists() {
        System.out.println("Following processors are registerd: ");
        for (Object value : processorList) {
            System.out.println(List.of(value));
        }

        System.out.println("Following sets are registerd: ");
        for (Object value : setList) {
            System.out.println(List.of(value));
        }

        System.out.println("Following sinks are registerd: ");
        for (Object value : sinkList) {
            System.out.println(List.of(value));
        }

        System.out.println("Following streams are registerd: ");
        for (Object value : streamList) {
            System.out.println(List.of(value));
        }
    }
}
