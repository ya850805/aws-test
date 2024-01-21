package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
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

        //upload to s3
//        String imageData = event.getBody();
        String imageData = "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAB0klEQVR42mJ0Hw7CIBBFyUdJUWFiEWIlE1HhkMNGo1GEKIAEEpJIQSBFWFDwSFEXEi6CG2VRN0UKFJiijKNpCCBRixDhwkJBQRFCHbXf6M/BztbH1+89zjw9v0FzyDnqmgE7jAhJBzjdPJ+NmOmjTgk61nLa9fcB1/cbNzq2QxRFUQ/IsJ3oxIghk4nKEmS4gIjZm6iXMjI/IdYwk8HnKX4+/bN4Obtf6vJZa7rQsmmnA5y4uZEx8F2I8PbGAI4G0sA/Z40otFpP+QWUnLw1fVzVq1bIyPjo6GpZycnLUajUbsuVIgIVpPRDxXVYfPn0a1WpBxaAvcvbuqEiIoLr5u3FjjjQ0FCsBQQFFqv11ddfeeklq3b792mJwBAdhFIuK1R5bPZt27bXZ2thYREVESKoaqoKCAhQVFTnXq1GmsS8fPnyZM2fO5fvvv3c3Nz6tSpE8Hl6+vuLfX19pyfHxsbGzabDabDioqKkq/KdHVVddtlll3W2LFjzVt2jRg0b9599tkTERF1a9fP4eHhIOLj4+9ra2iILi0uTieffJJPP/64oj7t27x7bfffsXLFiBDvvfee8+++/PtlszZk2v4lKlStjsQp/6KLL9ox44d1ddff/3r3bs2KFSssbA0NFVt2/fxYsX69++vRokWLkJCghEhKTq1Knfffde/fTTTz9nzZmTJjfeekuNHj5YsOHCe9///3Ktm3bplzzjuU9kZGQwJCPj5+Xl+ePHiSmpqqoqLCwMLCwserq6uqo7du7JiYm4ubk5io6OVm/f3+rWrl27t1bvfu3dV+5cmTZt2kBKS0tL+/PntL4mKiqqqq6mpqqvvfeey9997To0aKVlZWXzxxReeff15fVVVcdo0aLbd26dSoc2b95cy5YtwrmPHjsWi1WvXz8ZGRkEDRokWK5XN9++62pffv2SUuLMzZk0KDAwM9e3bl19++WX69OnTmj16tNZZ5557jyyy9DXl5eWlqaqioqKmpqqm4uDiJiIiQwJChLlizTpk1rTII4/MVqt9vjxo3SyZMkWLFiBFxcXFy/fp1Zs+ePd69epevnllzRokWLyu3btysnJUuWLFh7du7dy5sycPXr0sMjKyv7Vr1yqN9//zr27t3rfPntWRI0c8tTTz9lvPPPzfu3LnZ2dm6dep06dIgkTJhhTUFCg0bNnTunXrp0e3duvXr06OEhIZFJmFjRgC/8c1eajRo2qjR4/W66+/Pm2++GKFuHHjRtT2Vq5cmZp3v37h99++ymqqokWLnhwoVpyYiICg8PV6vVq1Xr16iU6dOByQkKCoqIiRSWKhioKBQKFSooK9e/erdu3bPP/54/PzBhhVVx99dVVr166t16tTpJhEshg5cqTJlypRUREFMBp+PBhMmTMCkiJiYmKPj4+UmprKwKFDBw+2YMECSkpKffv2id+/ebBggWT+fPn2dDpdn9+vVLvjiiyul2223q6ur66quv5cqV06aNEipqak6duyYtW7b8RUREmIsWLdLmzRsnJyclS5ciF1797dm3b1JUVFRFx99dmhoqLi7uuut69at8fjjj/ttttuOvvfeey3334KCAhQVZVVls02b95ctm7dijBkzNvHx8cWFhVq9ePUZGhmJiYqKmpqZGRkNOnT5cuHBhTJ05shISEaNWqUmrV6vEiBE0Os2bN3lypXCxcuJDp06KCoqEsDBw99thjFBcXp0+fTnZ2tqamrqwIED4+/tnZ2dj8eLFpKSkqKioKlTp1iu1gkkUmMwOXL19q0aKCsra1UqVNB1apVVau3at2LBhCunfvrlWrVig3NzdTUVHvvfeee9+++2rlypWp0wZ9++fTw8PPrqq+q6cOFDd+/e9+tOfn5+6enp8+fMlRSJEnNmDEiBIoKSkxMTG06OjooKFDt2rXrqqaeusP/++6+nXr1v39/fz99ttvTggw+sdOnT2LFj7NixQqNHjwoLKyotX75cGpqqoqCgIK\n";
//        String imageData = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAB0klEQVR42mJ0Hw7CIBBFyUdJUWFiEWIlE1HhkMNGo1GEKIAEEpJIQSBFWFDwSFEXEi6CG2VRN0UKFJiijKNpCCBRixDhwkJBQRFCHbXf6M/BztbH1+89zjw9v0FzyDnqmgE7jAhJBzjdPJ+NmOmjTgk61nLa9fcB1/cbNzq2QxRFUQ/IsJ3oxIghk4nKEmS4gIjZm6iXMjI/IdYwk8HnKX4+/bN4Obtf6vJZa7rQsmmnA5y4uDiJiIiQwJChLlizTpk1rTII4/MVqt9vjxo3SyZMkWLFiBFxcXFy/fp1Zs+ePd69epevnllzRokWLyu3btysnJUuWLFh7du7dy5sycPXr0sMjKyv7Vr1yqN9//zr27t3rfPntWRI0c8tTTz9lvPPPzfu3LnZ2dm6dep06dIgkTJhhTUFCg0bNnTunXrp0e3duvXr06OEhIZFJmFjRgC/8c1eajRo2qjR4/W66+/Pm2++GKFuHHjRtT2Vq5cmZp3v37h99++ymqqokWLnhwoVpyYiICg8PV6vVq1Xr16iU6dOByQkKCoqIiRSWKhioKBQKFSooK9e/erdu3bPP/54/PzBhhVVx99dVVr166t16tTpJhEshg5cqTJlypRUREFMBp+PBhMmTMCkiJiYmKPj4+UmprKwKFDBw+2YMECSkpKffv2id+/ebBggWT+fPn2dDpdn9+vVLvjiiyul2223q6ur66quv5cqV06aNEipqak6duyYtW7b8RUREmIsWLdLmzRsnJyclS5ciF1797dm3b1JUVFRFx99dmhoqLi7uuut69at8fjjj/ttttuOvvfeey3334KCAhQVZVVls02b95ctm7dijBkzNvHx8cWFhVq9ePUZGhmJiYqKmpqZGRkNOnT5cuHBhTJ05shISEaNWqUmrV6vEiBE0Os2bN3lypXCxcuJDp06KCoqEsDBw99thjFBcXp0+fTnZ2tqamrqwIED4+/tnZ2dj8eLFpKSkqKioKlTp1iu1gkkUmMwOXL19q0aKCsra1UqVNB1apVVau3at2LBhCunfvrlWrVig3NzdTUVHvvfeee9+++2rlypWp0wZ9++fTw8PPrqq+q6cOFDd+/e9+tOfn5+6enp8+fMlRSJEnNmDEiBIoKSkxMTG06OjooKFDt2rXrqqaeusP/++6+nXr1v39/fz99ttvTggw+sdOnT2LFj7NixQqNHjwoLKyotX75cGpqqoqCgIK\n";
//        String imageData = "TFMwdExTMHRMUzB0TFMwdExTMHRMUzB0TFMwdExTMHRMUzB0TFRZd01ETTROemMzT0RJM09ETXdNalk0TVRBMk56UXlPQTBLUTI5dWRHVnVkQzFFYVhOd2IzTnBkR2x2YmpvZ1ptOXliUzFrWVhSaE95QnVZVzFsUFNKbWFXeGxJanNnWm1sc1pXNWhiV1U5SW1samIyNXpPQzFtWVhacFkyOXVMVFE0TG5CdVp5SU5Da052Ym5SbGJuUXRWSGx3WlRvZ2FXMWhaMlV2Y0c1bkRRb05Db2xRVGtjTkNob0tBQUFBRFVsSVJGSUFBQUF3QUFBQU1BZ0dBQUFBVndMNWh3QUFBQWx3U0ZsekFBQUxFd0FBQ3hNQkFKcWNHQUFBQkw5SlJFRlVlSnp0V1Z0c0ZWVVVYVUFMRmFPaGdWS01mcUN0d0FkVWlnM0VpQVVVUXVLdmhLUThoSVJIUW9rUkNQeUJJRWlVSURYQlVHdE5FNmhFb2tBYkNKQVFTRlVJaVQ4cUtHbU5WT1g5S0tEeVVDbUZYclBOT21ZN21Ubm56TDFEZTAyNmtzbTljODZhUFdmTm5MUDNQbnVBSHZRZ0s5QVB3RVFBYndKb0FOQU00RHFBdXp5dXM2MkJuQWtBK2lJTE1BYkFod0IrQTVDS2VjZzFOUUJLdTJQZ293RWNDQXlvRGNCbUFCVVVWcWo2Q3RsV1FVNmI2dXNFc0E5QVNWY01QQTlBRllBTzN2eTIrcjhraEc4R0djUlN0bmZRaHZuL0x1L3hRUEEwZ09QcVp2TDZKL0g4RHdENU1RVGs4NW9VYmNnMHZNZnpid0FVSnozNE12WGF6d0I0Z2UzcjJGWWJjVjJVQUVFZCs5YnlmQnlBbjlnbWkvNjVwQVkvRnNCTkdqNEVZSURxTXpjc1QwT0FlSzBVYmVnM2M1anROL25nTXA0MlYybXdrZTdTWUFUYmZ3ZVFtNFlBdWVZRys0ZXJkbGtEZTVSaktFcDM4SGxxemplRkRISXgrM1paYk5nRUNIYXp2ekxRTHZmNlFxMkp0QloyRlEzOEFtQlFTSDhEKytkbklHQSsrOFZXRUlONGIrbmZtSTZmTis0eEc0NE9BS1BpQ0FnR3FXdzQ5dm9PZmd5am82aCtLb0l6bVVhUE9XeTVwaEJvSTBXYllTamlXRG81TTV5b29jRjZDMmNoT2RzU0VGQlB6Z0lMWnpzNTFRNWIvN2hKazVnOWIrRzlRODZxQkFTOFFjN2JGazY1Q25CUkx2cy93VVZXZnk4TGJ5ZDVrcHhsS21BbU9aOWFPREtXMCtTWkxDQVVhMGlTM01TR0w4bDdNUUVCTDVIenVZUDNFWG1yYlNUajIyYzVqSDFMM3JNSkNDaFRBY3VHT2VSSjhJdEVzK2ZBVEE1VW5JQ0FZbkphUFhJeTRaMjBrVXplVStBd1puZ0RFeEJRUUk3WXRLRlE1VWVSYU0rQ2dKVnlITzMvZHdGLzJRUmM4NXhDMXhLY1F2M0prUjJhRFkrUmQ5WkdhaUhKRmJKL0ppOHExWWdqSUorY1h6MFhzWGpBU0RSNnV0RVRua0o5QlBUaG9IWTRlRE5vUzRLb001Qzk3ekIybER3cFVHVXF3QmZWdExYY1Jwcmc2Wk4zZUw0cEh3RWxBTjRDOExpRDEreVRTdlRsWEhRRk01UE1yVXhBUUszYVd5K0l5TUZLeWJrRUlNZGhEeCtRTExsSEZDbzlPTDRDVmdUY1pGUElSbjR6Kzk2REIwcTVlWGpRL2x6V2tXQUt6OFUxWDFFdWRSa1grQkNlZDhiWlZ1N3JBZ0ZIZUsvQlBML0IrR00yTDNKOEJlQXovcGRmYjVSd0czZWZUeWdNWjJoNFpBSmU2QUo1WnVxOHpJQ2xCVCtEbU5qSUN5L3lLUVZobnRUckNRallUOTRycXUxUnVrNHpuYjluTVBOR0huTjBzOWw0S0dKZnZEY0JBZXNEZGRMZ2R2Skg5a3NCZUJQVER5OFVxYUx1d1VDRmJJU2F1emtaQ3BqdWVCank4RGFvV2xVcnE5cGVLRlBGM2FiQWRHcHhSR1JmQWNOOEVqVUFzL25KS3NXcE5UZU9DT1BpenF0b2FDb0tIMmNvb0RlQVd5RVo3c044TzNVaGkxb0V6RU1NU09iNU5TOFc3N1NWbTNxVG93L01NQmM2RmlnVTVLamlnVGt1ODJITlpueUlqWDcwVHVZMS9xbm01YktZQXVTQnlEWXhtS3daTzV2VTlmZFl1Yk9WZW1KaEZCZWNqdGh0ekdKbk1JL1NIL21lQURBZXdLc2NxQ2tLbkFyeGFsS3BtMGJiN1NwMWQ2WGJhVUgyQTF0WU1Zc2JpU1ZwbERVVTNMQ2NVMDdqTlFCUHF2V2g0MFNpeU9VVFhzMFBIaWY1Um1TcTNXSEY0VHZXblZheEFoZ3NFZlpYSC9yaytFVDFMV0xiRlkvdGJyZWltUVA5QWNBanFyMFg0MUNLdThhc3hRb09Ybjh6TXhpcXB0YlViaGhiSWhEM0tkUHgzNitpZndPTG1LSXdGSTJzYUFBQUFBQkpSVTVFcmtKZ2dnMEtMUzB0TFMwdExTMHRMUzB0TFMwdExTMHRMUzB0TFMwdExTMHRMVFl3TURNNE56YzNPREkzT0RNd01qWTRNVEEyTnpReU9DMHREUW89";

        logger.log("imageData=" + imageData);
//        byte[] imageBytes = imageData.getBytes();

        byte[] binaryData = Base64.getDecoder().decode(imageData);

        String objectName = UUID.randomUUID() + ".png";
        String s3ObjectPath = "images/" + objectName;

        String bucketName = "jason-test-upload-file";
        Region region = Region.AP_NORTHEAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        putS3Object(s3, bucketName, s3ObjectPath, binaryData);
//        putS3Object(s3, bucketName, s3ObjectPath, imageData);
        URL signedUrlForStringPut = createSignedUrlForStringPut(bucketName, objectName);

        //send sns
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
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            metadata.put("Content-Type", "image/png");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

//            s3.putObject(putOb, RequestBody.fromBytes(object));
            s3.putObject(putOb, RequestBody.fromByteBuffer(ByteBuffer.wrap(object)));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void putS3Object(S3Client s3, String bucketName, String objectKey, String content) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            metadata.put("Content-Type", "image/png");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            s3.putObject(putOb, RequestBody.fromString(content));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private URL createSignedUrlForStringPut(String bucketName, String keyName) {
        try (S3Presigner presigner = S3Presigner.create()) {

            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .contentType("text/plain")
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(10)) // The URL will expire in 10 minutes.
                    .putObjectRequest(objectRequest)
                    .build();

            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
//            String myURL = presignedRequest.url().toString();

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
