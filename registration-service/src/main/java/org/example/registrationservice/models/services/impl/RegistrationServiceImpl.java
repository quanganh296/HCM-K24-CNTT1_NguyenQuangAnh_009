package org.example.registrationservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.registrationservice.models.constants.RegistrationStatus;
import org.example.registrationservice.models.dto.requests.CreateRegistrationRequest;
import org.example.registrationservice.models.dto.responses.EventResponse;
import org.example.registrationservice.models.dto.responses.RegistrationDetailResponse;
import org.example.registrationservice.models.dto.responses.RegistrationResponse;
import org.example.registrationservice.models.entities.Registration;
import org.example.registrationservice.models.entities.RegistrationDetail;
import org.example.registrationservice.models.repositories.RegistrationDetailRepository;
import org.example.registrationservice.models.repositories.RegistrationRepository;
import org.example.registrationservice.models.services.RegistrationService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RegistrationServiceImpl implements RegistrationService {
    private final RegistrationRepository registrationRepository;
    private final RegistrationDetailRepository registrationDetailRepository;
    private final EventGatewayService eventGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    @Transactional
    public RegistrationResponse createRegistration(CreateRegistrationRequest request) {
        Registration registration = new Registration();
        registration.setParticipantName(request.participantName());
        registration.setParticipantEmail(request.participantEmail());
        registration.setStatus(RegistrationStatus.PENDING);

        Double totalAmount = 0.0;
        List<RegistrationDetail> detailList = new ArrayList<>();

        for (var item : request.items()) {
            EventResponse event = eventGatewayService.getEventById(item.eventId());

            if (item.quantity() == null || item.quantity() <= 0
                    || event.availableTickets() == null || item.quantity() > event.availableTickets()) {
                throw new IllegalArgumentException("Invalid quantity or insufficient tickets for event: " + item.eventId());
            }
            Double ticketPrice = event.ticketPrice();
            Double lineTotal = ticketPrice * item.quantity();
            totalAmount += lineTotal;

            RegistrationDetail detail = new RegistrationDetail();
            detail.setEventId(item.eventId());
            detail.setQuantity(item.quantity());
            detail.setTicketPrice(ticketPrice);
            detail.setLineTotal(lineTotal);
            detailList.add(detail);

        }

        registration.setTotalAmount(totalAmount);
        Registration savedRegistration = registrationRepository.save(registration);

        for (RegistrationDetail detail : detailList) {
            detail.setRegistration(savedRegistration);
        }
        List<RegistrationDetail> savedDetails = registrationDetailRepository.saveAll(detailList);

        kafkaTemplate.send("registration-created", savedRegistration.getParticipantEmail());

        return mapToRegistrationResponse(savedRegistration, savedDetails);

    }
       //throw new UnsupportedOperationException();



    private RegistrationResponse mapToRegistrationResponse(Registration registration, List<RegistrationDetail> details) {
        List<RegistrationDetailResponse> itemResponses = details.stream()
                .map(detail -> new RegistrationDetailResponse(
                detail.getId(),
                detail.getEventId(),
                detail.getQuantity(),
                detail.getTicketPrice(),
                detail.getLineTotal()
        )).toList();

        return new RegistrationResponse(
                registration.getId(),
                registration.getParticipantName(),
                registration.getParticipantEmail(),
                registration.getTotalAmount(),
                registration.getStatus(),
                itemResponses
        );


}
