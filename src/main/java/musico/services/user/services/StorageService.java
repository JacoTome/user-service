package musico.services.user.services;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.gridfs.model.GridFSFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import musico.services.user.models.ProfilePicture;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.bson.BsonBinarySubType;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsObject;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.data.mongodb.gridfs.GridFsResource;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RequiredArgsConstructor
@Service
@Slf4j
public class StorageService {
    private final GridFsTemplate template;
    private final GridFsOperations gridFsOperations;

    public String storeProfilePicture(String userId, MultipartFile image) throws IOException {
        // store the image in the database
        DBObject metadata = new BasicDBObject();
        metadata.put("fileSize", image.getSize());
        if(template.findOne(new Query(Criteria.where("filename").is(userId))) != null) {
            template.delete(new Query(Criteria.where("filename").is(userId)));
        }
        ObjectId id = template.store(image.getInputStream(), userId, image.getContentType(), metadata);
        return id.toString();
    }

    public ProfilePicture getProfilePicture(String userId) throws IOException {
        GridFSFile file = template.findOne(new Query(Criteria.where("filename").is(userId)));
        if (file == null || file.getMetadata() == null) {
            return null;
        }
        return ProfilePicture.builder()
                .userId(file.getFilename())
                .fileSize(file.getMetadata().get("fileSize").toString())
                .image(gridFsOperations.getResource(file).getInputStream().readAllBytes())
                .build();
    }

    public String storeAudio (String userId, MultipartFile audio) throws IOException {
        DBObject metadata = new BasicDBObject();
        metadata.put("fileSize", audio.getSize());
        if(template.findOne(new Query(Criteria.where("filename").is(userId + "_audio"))) != null) {
            template.delete(new Query(Criteria.where("filename").is(userId + "_audio")));
        }
        ObjectId id = template.store(audio.getInputStream(), userId + "_audio", audio.getContentType(), metadata);
        return id.toString();
    }

    public ProfilePicture getAudio(String name) throws IOException {
        GridFSFile file = template.findOne(new Query(Criteria.where("filename").is(name + "_audio")));
        if (file == null || file.getMetadata() == null) {
            return null;
        }
        return ProfilePicture.builder()
                .userId(file.getFilename())
                .fileSize(file.getMetadata().get("fileSize").toString())
                .image(gridFsOperations.getResource(file).getInputStream().readAllBytes())
                .build();
    }
}
