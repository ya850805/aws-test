package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Deprecated
public class FileUploadHandler implements RequestStreamHandler {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        try {
            // 解析HTTP事件的输入流
            JsonNode eventNode = objectMapper.readTree(input);

            // 获取文件内容
            String fileContent = eventNode.get("body").asText();

            // 在这里处理文件内容，可以保存到S3等地方
            // ...

            // 构建响应
            String responseMessage = "File uploaded successfully";
            byte[] responseBytes = responseMessage.getBytes("UTF-8");
            output.write(responseBytes);

        } catch (Exception e) {
            // 处理异常情况
            String errorMessage = "Error processing file upload: " + e.getMessage();
            byte[] errorBytes = errorMessage.getBytes("UTF-8");
            output.write(errorBytes);
        }
    }
}