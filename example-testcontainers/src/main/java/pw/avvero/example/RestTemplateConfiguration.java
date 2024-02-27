package pw.avvero.example;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.zalando.logbook.spring.LogbookClientHttpRequestInterceptor;

import java.util.Collections;

@Configuration
public class RestTemplateConfiguration {

    @Bean
    public RestTemplate restTemplate(LogbookClientHttpRequestInterceptor interceptor) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setInterceptors(Collections.singletonList(interceptor));
        return restTemplate;
    }

}
