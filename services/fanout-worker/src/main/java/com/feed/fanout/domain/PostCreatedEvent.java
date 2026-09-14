package com.feed.fanout.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostCreatedEvent {
    public String postId;
    public String userId;
    public String mediaUrl;
    public int mediaType;
    public String caption;
    public String createdAt;
}
