import type { IdMinter, NotificationRepository, PostCreatedEvent, UsernameDirectory } from '../domain/types.js';

const MENTION_RE = /@([a-zA-Z0-9_]+)/g;

export function extractMentions(caption: string): string[] {
  const usernames = new Set<string>();
  for (const match of caption.matchAll(MENTION_RE)) {
    usernames.add(match[1]);
  }
  return [...usernames];
}

export class NotificationService {
  constructor(
    private readonly usernames: UsernameDirectory,
    private readonly notifications: NotificationRepository,
    private readonly minter: IdMinter,
  ) {}

  /**
   * Resolves each @mention to a user_id via the Redis directory (not a
   * Postgres query -- see doc/sharding.md, username isn't the shard
   * key), then writes the notification to the RECIPIENT's shard. The
   * notification's own ID inherits that same shard, mirroring how a
   * comment inherits its post's shard in post-ingestion-service.
   */
  async handlePostCreated(event: PostCreatedEvent): Promise<string[]> {
    const mentions = extractMentions(event.caption ?? '');
    const notified: string[] = [];

    for (const username of mentions) {
      const recipientId = await this.usernames.lookup(username);
      if (recipientId === null) continue;

      const actorId = BigInt(event.userId);
      if (recipientId === actorId) continue; // no self-notifications

      await this.notifications.create(recipientId, actorId, BigInt(event.postId));
      notified.push(recipientId.toString());
    }
    return notified;
  }
}
