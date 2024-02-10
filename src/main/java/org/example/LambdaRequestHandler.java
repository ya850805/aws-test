package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author jason
 * @description
 * @create 2024/1/21 00:30
 **/
public class LambdaRequestHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("EVENT TYPE: " + event.getClass().toString());

        logger.log("event = " + event);

        //1. upload to s3
        String imageData = event.getBody();
        logger.log("imageData=" + imageData);

        byte[] binaryData = Base64.getDecoder().decode(imageData);

        String objectName = UUID.randomUUID() + ".png";
        String s3ObjectPath = "images/" + objectName;

        String bucketName = "jason-test-upload-file-java";
        Region region = Region.AP_NORTHEAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        putS3Object(s3, bucketName, s3ObjectPath, binaryData);
        logger.log("bucketName = " + bucketName);
        logger.log("objectName = " + objectName);

        URL signedUrlForStringPut = createSignedUrlForStringPut(bucketName, objectName);

        //2. send sns
        String message = signedUrlForStringPut.toString();
        logger.log("url=" + message);
        String topicArn = "arn:aws:sns:ap-northeast-1:027280691076:Deeplearnaws_SNS_Topic";
        SnsClient snsClient = SnsClient.builder()
                .region(Region.AP_NORTHEAST_1)
                .build();
        pubTopic(snsClient, message, topicArn);
        snsClient.close();

        APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();
        response.setIsBase64Encoded(false);
        response.setStatusCode(200);
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/json");
        response.setHeaders(headers);
        response.setBody("ok!");
        return response;
    }

    public void putS3Object(S3Client s3, String bucketName, String objectKey, byte[] object) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .contentType("image/png")
                    .acl("public-read")
                    .build();

            s3.putObject(putOb, RequestBody.fromBytes(object));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public URL createSignedUrlForStringPut(String bucketName, String keyName) {
        try (S3Presigner presigner = S3Presigner.create()) {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .contentType("image/png")
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(10))  // The URL will expire in 10 minutes.
                    .putObjectRequest(objectRequest)
                    .build();

            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
            return presignedRequest.url();
        }
    }

    private void pubTopic(SnsClient snsClient, String message, String topicArn) {
        try {
            PublishRequest request = PublishRequest.builder()
                    .message(message)
                    .topicArn(topicArn)
                    .build();

            PublishResponse result = snsClient.publish(request);
        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

//    @Override
//    public String handleRequest(String s, Context context) {
//        LambdaLogger logger = context.getLogger();
//        logger.log("String found: " + s);
//        return "接收到：" + s;
//    }
}
