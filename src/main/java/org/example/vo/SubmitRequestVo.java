package org.example.vo;

import lombok.Data;

/**
 * @author jason
 * @description
 * @create 2024/1/27 02:04
 **/
@Data
public class SubmitRequestVo {
    private String title;
    private String name;
    private String email;
    private String[] photos;
}
