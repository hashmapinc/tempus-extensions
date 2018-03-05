package com.hashmapinc.tempus.handler;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.exception.PublishMsgException;

public interface BaseStreamHandler {
    void onMessage(Message message) throws PublishMsgException;
}
