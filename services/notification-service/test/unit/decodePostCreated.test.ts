import { describe, expect, it } from '@jest/globals';
import { Builder, ByteBuffer } from 'flatbuffers';

import { decodePostCreated } from '../../src/kafka/decodePostCreated.js';
import { MediaType } from '../../src/genfbs/feed/events/media-type.js';
import { PostCreatedEvent as FbPostCreatedEvent } from '../../src/genfbs/feed/events/post-created-event.js';

function buildPostCreatedFb(fields: {
  postId: bigint;
  userId: bigint;
  mediaUrl: string;
  mediaType: MediaType;
  caption: string;
  createdAt: bigint;
}): Uint8Array {
  const builder = new Builder(0);
  const mediaUrlOff = builder.createString(fields.mediaUrl);
  const captionOff = builder.createString(fields.caption);

  FbPostCreatedEvent.startPostCreatedEvent(builder);
  FbPostCreatedEvent.addPostId(builder, fields.postId);
  FbPostCreatedEvent.addUserId(builder, fields.userId);
  FbPostCreatedEvent.addMediaUrl(builder, mediaUrlOff);
  FbPostCreatedEvent.addMediaType(builder, fields.mediaType);
  FbPostCreatedEvent.addCaption(builder, captionOff);
  FbPostCreatedEvent.addCreatedAt(builder, fields.createdAt);
  const offset = FbPostCreatedEvent.endPostCreatedEvent(builder);

  builder.finish(offset);
  return builder.asUint8Array();
}

// Round-trip proof against the actual wire format, mirroring this
// repo's "verify it, don't assert it" standard (see
// scripts/verify_shard_parity.sh) -- none of the JSON-era tests ever
// exercised real (de)serialization, only the language-native object.
describe('decodePostCreated', () => {
  it('decodes every field from a real FlatBuffer, including 64-bit IDs as bigint', () => {
    const raw = buildPostCreatedFb({
      postId: 357748213593194496n,
      userId: 357748214239117312n,
      mediaUrl: 'https://example.com/img.jpg',
      mediaType: MediaType.Image,
      caption: 'hello @user_3',
      createdAt: 1735689600000n,
    });

    const event = decodePostCreated(raw);

    expect(event.postId).toBe(357748213593194496n);
    expect(event.userId).toBe(357748214239117312n);
    expect(event.mediaUrl).toBe('https://example.com/img.jpg');
    expect(event.mediaType).toBe(MediaType.Image);
    expect(event.caption).toBe('hello @user_3');
    expect(event.createdAt).toBe(1735689600000);
  });

  it('decodes an empty caption as an empty string, not null', () => {
    const raw = buildPostCreatedFb({
      postId: 1n,
      userId: 2n,
      mediaUrl: 'https://example.com/a.jpg',
      mediaType: MediaType.Video,
      caption: '',
      createdAt: 0n,
    });

    expect(decodePostCreated(raw).caption).toBe('');
  });
});
