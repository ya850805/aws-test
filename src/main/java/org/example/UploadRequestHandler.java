package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
public class UploadRequestHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("EVENT TYPE: " + event.getClass().toString());

        logger.log("event = " + event);
        logger.log("headers = " + event.getHeaders());

        Map<String, String> headers = event.getHeaders();
        String requestContentType = headers.get("content-type");
        String email = headers.get("email");
        String fileFullName = headers.get("filename");
        String fileName = fileFullName.split("\\.")[0];
        String extension = fileFullName.split("\\.")[1];
        logger.log("requestContentType = " + requestContentType);
        logger.log("email = " + email);
        logger.log("fileFullName = " + fileFullName);

        //1. upload to s3
        String imageData = event.getBody();
        logger.log("imageData=" + imageData);

        byte[] binaryData = Base64.getDecoder().decode(imageData);

        String objectName = fileName + "-" + System.currentTimeMillis() + "." + extension;
        String s3ObjectPath = email + "/" + objectName;

        String bucketName = "jason-test-upload-file-java-2";
        Region region = Region.AP_NORTHEAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        putS3Object(s3, bucketName, s3ObjectPath, binaryData, requestContentType);
        logger.log("bucketName = " + bucketName);
        logger.log("objectName = " + objectName);

        //2. generate presigned url
        S3Presigner presigner = S3Presigner.builder()
                .region(region)
                .build();
        String presignedUrl = getPresignedUrl(presigner, bucketName, s3ObjectPath);
        logger.log("presignedUrl = " + presignedUrl);

        //3. send sns
//        String message = presignedUrl.toString();
//        String topicArn = "arn:aws:sns:ap-northeast-1:027280691076:Deeplearnaws_SNS_Topic";
//        SnsClient snsClient = SnsClient.builder()
//                .region(Region.AP_NORTHEAST_1)
//                .build();
//        pubTopic(snsClient, message, topicArn);
//        snsClient.close();

        APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();
        response.setIsBase64Encoded(false);
        response.setStatusCode(200);
        HashMap<String, String> resHeaders = new HashMap<String, String>();
        resHeaders.put("Content-Type", "application/json");
        resHeaders.put("Access-Control-Allow-Origin", "*");
        resHeaders.put("Access-Control-Allow-Methods", "*");
        resHeaders.put("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,filename,email");
        response.setHeaders(resHeaders);
        response.setBody(presignedUrl);
        return response;
    }

    public void putS3Object(S3Client s3, String bucketName, String objectKey, byte[] object, String contentType) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(Map.of("Content-Type", contentType))
                    .contentType(contentType)
                    .build();

            s3.putObject(putOb, RequestBody.fromBytes(object));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public String getPresignedUrl(S3Presigner presigner, String bucketName, String keyName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofDays(7))
                    .getObjectRequest(getObjectRequest)
                    .build();

            PresignedGetObjectRequest presignedGetObjectRequest = presigner.presignGetObject(getObjectPresignRequest);
            return presignedGetObjectRequest.url().toString();
        } catch (S3Exception e) {
            e.getStackTrace();
        }

        return "";
    }

//    private void pubTopic(SnsClient snsClient, String message, String topicArn) {
//        try {
//            PublishRequest request = PublishRequest.builder()
//                    .message(message)
//                    .topicArn(topicArn)
//                    .build();
//
//            PublishResponse result = snsClient.publish(request);
//        } catch (SnsException e) {
//            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1);
//        }
//    }
}
