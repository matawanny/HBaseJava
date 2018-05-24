package com.yieldbook.mortgage.hbase.notify;

import java.util.ArrayList;
import java.util.List;

public class Notification {

    public enum NotificationType{
        COMMENT,
        LIKE,
        FRIEND_REQUEST,
        PRIVATE_MESSAGE
    }

    public enum ResourceType {
        PHOTO,
        LINK
    }

    public List<Column> getColumns() {
        return columns;
    }

    List<Column> columns = new ArrayList<>();

    NotificationType type;

    public NotificationType getType() {
        return this.type;
    }

    public String toString(){
        return "General Notification";
    }

}



