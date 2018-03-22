package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.exception.PublishMsgException;
import com.hashmapinc.tempus.handler.BaseStreamHandler;

import java.util.ArrayList;
import java.util.List;

public class TestStreamHandler implements BaseStreamHandler {
    private ArrayList<Message> messages =  new ArrayList<>();
    @Override
    public void onMessage(Message message) throws PublishMsgException {
        try {
            messages.add(message);
        } catch (Exception ex) {
            throw new PublishMsgException(ex);
        }
    }

    public ArrayList<Message> getMessages() {
        return messages;
    }
}
