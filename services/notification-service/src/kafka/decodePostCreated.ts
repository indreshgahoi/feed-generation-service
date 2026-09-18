import { ByteBuffer } from 'flatbuffers';

import type { PostCreatedEvent } from '../domain/types.js';
import { PostCreatedEvent as FbPostCreatedEvent } from '../genfbs/feed/events/post-created-event.js';

/**
 * Decodes the FlatBuffers `post-created` event (schemas/fbs/post_created.fbs)
 * into the plain domain.PostCreatedEvent NotificationService already
 * knows how to handle -- the same boundary the old JSON.parse path used.
 * See doc/DESIGN.md for why this event is FlatBuffers, not JSON,
 * on the wire.
 */
export function decodePostCreated(raw: Uint8Array): PostCreatedEvent {
  const fb = FbPostCreatedEvent.getRootAsPostCreatedEvent(new ByteBuffer(raw));
  return {
    postId: fb.postId(),
    userId: fb.userId(),
    mediaUrl: fb.mediaUrl() ?? '',
    mediaType: fb.mediaType(),
    caption: fb.caption() ?? '',
    createdAt: Number(fb.createdAt()),
  };
}
