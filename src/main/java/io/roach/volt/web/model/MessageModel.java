package io.roach.volt.web.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.springframework.hateoas.RepresentationModel;

@JsonPropertyOrder({"links"})
public class MessageModel extends RepresentationModel<MessageModel> {
    public static MessageModel from(String message) {
        return new MessageModel(message);
    }

    private String message;

    private String notice;

    public MessageModel() {
    }

    public MessageModel(String message) {
        this.message = message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getNotice() {
        return notice;
    }

    public void setNotice(String notice) {
        this.notice = notice;
    }
}
