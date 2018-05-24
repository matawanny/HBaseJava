package com.yieldbook.mortgage.hbase.notify;


public class FriendRequestNotification extends Notification{

    NotificationType type = NotificationType.FRIEND_REQUEST;

    FriendRequestNotification(String fromUser,String user) {
      this.columns.add(new Column("attributes", "type", NotificationType.FRIEND_REQUEST.toString()));
      this.columns.add(new Column("attributes", "from_user", fromUser));
      this.columns.add(new Column("attributes", "for_user", user));
  }

    public String toString(){
        return "You have a friend request from "+this.columns.get(1).value;
    }
}
