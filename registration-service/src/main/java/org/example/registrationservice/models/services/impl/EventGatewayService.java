package org.example.registrationservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.registrationservice.clients.EventClient;
import org.example.registrationservice.exceptions.EventNotFoundException;
import org.example.registrationservice.models.dto.responses.EventResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventGatewayService {
    private final EventClient eventClient;

    @CircuitBreaker(name = "eventService", fallbackMethod = "getEventFallback")
    public EventResponse getEventById(Long eventId)  {
        try{
            EventResponse event = eventClient.getEventById(eventId);
            if(event == null) {
                throw new EventNotFoundException(eventId);
            }
            return  event;
        } catch (FeignException.NotFound e) {
            throw new EventNotFoundException(eventId);
        }

    }

    public EventResponse getEventFallback(Long eventId, Throwable throwable) {
        if(throwable instanceof EventNotFoundException ex) {
            throw ex;
        }
        if(throwable instanceof FeignException.NotFound) {
            throw new EventNotFoundException(eventId);
        }
        throw new UnsupportedOperationException();
    }
}
