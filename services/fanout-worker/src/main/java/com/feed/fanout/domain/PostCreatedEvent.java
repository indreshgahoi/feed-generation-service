package com.feed.fanout.domain;

/**
 * Plain domain holder the service layer works with -- deliberately
 * ignorant of the wire format. FanoutWorkerApp decodes the FlatBuffers
 * `post-created` event (schemas/fbs/post_created.fbs) into one of these
 * before calling FanoutService, the same POJO boundary the old
 * Jackson-based JSON path used. See doc/wire-protocols.md.
 *
 * postId/userId are native longs and createdAt is unix epoch millis --
 * matching the wire schema directly, instead of the JSON path's
 * stringified IDs and RFC3339 timestamp.
 */
public class PostCreatedEvent {
    public long postId;
    public long userId;
    public String mediaUrl;
    public int mediaType;
    public String caption;
    public long createdAt;
}
