package musico.services.user.controller;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import musico.services.user.models.ProfilePicture;
import musico.services.user.models.UserProfileDTO;
import musico.services.user.services.StorageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.security.Principal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
@RequestMapping(path = "/user/profile")
public class ProfileController {

    private final KafkaTemplate<String, UserProfileDTO> kafkaTemplate;
    private final ReplyingKafkaTemplate<String, UserProfileDTO, UserProfileDTO> replyingKafkaTemplate;
    private final StorageService storageService;

    public ProfileController(@Qualifier("kafkaTemplateAuthProcess") KafkaTemplate<String, UserProfileDTO> kafkaTemplate,
                             ReplyingKafkaTemplate<String, UserProfileDTO, UserProfileDTO> replyingKafkaTemplate,
                             StorageService storageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.storageService = storageService;
    }

    @PostMapping(path = "/create")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public String createProfile(@RequestBody UserProfileDTO profileDTO, HttpServletRequest principal) {
        // Get User ID from principal
        profileDTO.setUserId(principal.getUserPrincipal().getName());
        kafkaTemplate.send("profile-creation", profileDTO);
        return "Profile created";
    }

    @GetMapping(path = "/get")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public ResponseEntity<UserProfileDTO> getProfile( HttpServletRequest principal) throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Principal: {}", principal);
        UserProfileDTO profileDTO = new UserProfileDTO();
        profileDTO.setUserId(principal.getUserPrincipal().getName());
        ProducerRecord<String, UserProfileDTO> record = new ProducerRecord<>("profile-get", profileDTO);
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "profile-get-response".getBytes()));
        RequestReplyFuture<String, UserProfileDTO, UserProfileDTO> future = replyingKafkaTemplate.sendAndReceive(record);
        future.getSendFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
        log.info("Sent: {}", record.value()) ;
        try{
            ConsumerRecord<String, UserProfileDTO> response = future.get(10, java.util.concurrent.TimeUnit.SECONDS);
            log.info("Response: {}", response.value());
            if(response.value().getUserId().equals("NOT_FOUND")){
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(response.value());
        }catch (Exception e){
            log.error("Error: {}", e.getMessage());
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping(path = "/profile-picture")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public String uploadProfilePicture(@RequestParam("file") MultipartFile file, HttpServletRequest principal) {
        try {
            String id = storageService.storeProfilePicture(principal.getUserPrincipal().getName(), file);
            log.info("Profile picture uploaded with ID: {}", id);
        } catch (Exception e) {
            log.error("Error uploading profile picture: {}", e.getMessage());
            return "Error uploading profile picture";
        }
        // Get User ID from principal
        return "Profile picture uploaded";
    }

    @GetMapping(path = "/profile-picture")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public ResponseEntity<ByteArrayResource> getProfilePicture(HttpServletRequest principal) {
        try {
            ProfilePicture profilePicture = storageService.getProfilePicture(principal.getUserPrincipal().getName());
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType("audio/mpeg"))
                    .headers(headers -> headers.setContentDispositionFormData("attachment", "profile.jpg"))
                    .body(new ByteArrayResource(profilePicture.getImage()));

        } catch (Exception e) {
            log.error("Error getting profile picture: {}", e.getMessage());
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping(path = "/audio")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public String uploadAudio(@RequestParam("file") MultipartFile file, HttpServletRequest principal) {
        try {
            String id = storageService.storeAudio(principal.getUserPrincipal().getName(), file);
            log.info("Audio uploaded with ID: {}", id);
        } catch (Exception e) {
            log.error("Error uploading audio: {}", e.getMessage());
            return "Error uploading audio";
        }
        // Get User ID from principal
        return "Audio uploaded";
    }

    @GetMapping(path = "/audio")
    @PreAuthorize("hasAuthority('SCOPE_user')")
    public ResponseEntity<ByteArrayResource> getAudio(HttpServletRequest principal) {
        try {
            ProfilePicture profilePicture = storageService.getAudio(principal.getUserPrincipal().getName());
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType("audio/mpeg"))
                    .headers(headers -> headers.setContentDispositionFormData("attachment", "audio.mp3"))
                    .body(new ByteArrayResource(profilePicture.getImage()));

        } catch (Exception e) {
            log.error("Error getting audio: {}", e.getMessage());
        }
        return ResponseEntity.notFound().build();
    }
}
