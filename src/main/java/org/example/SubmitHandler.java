package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.vo.SubmitRequestVo;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.util.Arrays;
import java.util.HashMap;

/**
 * @author jason
 * @description
 * @create 2024/1/27 01:48
 **/
public class SubmitHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("event = " + event);

        String subject = "收到請求～";
        StringBuilder message = new StringBuilder();

        try {
            SubmitRequestVo vo = objectMapper.readValue(event.getBody(), SubmitRequestVo.class);
            message.append("名稱：" + vo.getName() + "\n");
            message.append("郵件：" + vo.getEmail() + "\n");
            message.append("上傳圖片：\n");

            for (String photo : vo.getPhotos()) {
                message.append("\t ✭" + photo + "\n");
            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        String topicArn = "arn:aws:sns:ap-northeast-1:027280691076:Deeplearnaws_SNS_Topic";
        SnsClient snsClient = SnsClient.builder()
                .region(Region.AP_NORTHEAST_1)
                .build();
        pubTopic(snsClient, subject, message.toString(), topicArn);
        snsClient.close();

        APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();
        response.setIsBase64Encoded(false);
        response.setStatusCode(200);
        HashMap<String, String> resHeaders = new HashMap<String, String>();
        resHeaders.put("Content-Type", "application/json");
        resHeaders.put("Access-Control-Allow-Origin", "*");
        resHeaders.put("Access-Control-Allow-Methods", "*");
        resHeaders.put("Access-Control-Allow-Headers", "Content-Type");
        response.setHeaders(resHeaders);
        response.setBody("ok!");
        return response;
    }

    private void pubTopic(SnsClient snsClient, String subject, String message, String topicArn) {
        try {
            PublishRequest request = PublishRequest.builder()
                    .subject(subject)
                    .message(message)
                    .topicArn(topicArn)
                    .build();

            PublishResponse result = snsClient.publish(request);
        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
