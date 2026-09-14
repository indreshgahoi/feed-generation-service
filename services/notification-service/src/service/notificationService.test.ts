import { describe, expect, it } from '@jest/globals';

import type { IdMinter, NotificationRepository, PostCreatedEvent, UsernameDirectory } from '../domain/types.js';
import { extractMentions, NotificationService } from './notificationService.js';

class FakeUsernameDirectory implements UsernameDirectory {
  constructor(private readonly entries: Map<string, bigint>) {}
  async lookup(username: string): Promise<bigint | null> {
    return this.entries.get(username) ?? null;
  }
}

class FakeNotificationRepository implements NotificationRepository {
  public created: Array<{ recipientId: bigint; actorId: bigint; postId: bigint }> = [];
  async create(recipientId: bigint, actorId: bigint, postId: bigint): Promise<void> {
    this.created.push({ recipientId, actorId, postId });
  }
}

class FakeIdMinter implements IdMinter {
  newIdInheritingShard(existingId: bigint): bigint {
    return existingId + 1n; // deterministic stand-in, good enough for these tests
  }
}

function event(overrides: Partial<PostCreatedEvent> = {}): PostCreatedEvent {
  return {
    postId: '100',
    userId: '2',
    mediaUrl: 'https://example.com/a.jpg',
    mediaType: 1,
    caption: 'hello',
    createdAt: '2024-01-01T00:00:00Z',
    ...overrides,
  };
}

describe('extractMentions', () => {
  it('extracts unique usernames', () => {
    expect(extractMentions('hi @alice and @bob, thanks @alice!')).toEqual(['alice', 'bob']);
  });

  it('returns an empty array for no mentions', () => {
    expect(extractMentions('no mentions here')).toEqual([]);
  });
});

describe('NotificationService.handlePostCreated', () => {
  it('creates a notification for a resolvable mention', async () => {
    const directory = new FakeUsernameDirectory(new Map([['alice', 5n]]));
    const repo = new FakeNotificationRepository();
    const service = new NotificationService(directory, repo, new FakeIdMinter());

    const notified = await service.handlePostCreated(event({ caption: 'hi @alice', userId: '2' }));

    expect(notified).toEqual(['5']);
    expect(repo.created).toEqual([{ recipientId: 5n, actorId: 2n, postId: 100n }]);
  });

  it('skips mentions of usernames not in the directory', async () => {
    const directory = new FakeUsernameDirectory(new Map());
    const repo = new FakeNotificationRepository();
    const service = new NotificationService(directory, repo, new FakeIdMinter());

    const notified = await service.handlePostCreated(event({ caption: 'hi @ghost' }));

    expect(notified).toEqual([]);
    expect(repo.created).toEqual([]);
  });

  it('never creates a self-notification', async () => {
    const directory = new FakeUsernameDirectory(new Map([['author', 2n]]));
    const repo = new FakeNotificationRepository();
    const service = new NotificationService(directory, repo, new FakeIdMinter());

    const notified = await service.handlePostCreated(event({ caption: 'hi @author', userId: '2' }));

    expect(notified).toEqual([]);
    expect(repo.created).toEqual([]);
  });

  it('handles multiple distinct mentions', async () => {
    const directory = new FakeUsernameDirectory(
      new Map([
        ['alice', 5n],
        ['bob', 7n],
      ]),
    );
    const repo = new FakeNotificationRepository();
    const service = new NotificationService(directory, repo, new FakeIdMinter());

    const notified = await service.handlePostCreated(event({ caption: 'hi @alice and @bob' }));

    expect(notified.sort()).toEqual(['5', '7']);
    expect(repo.created).toHaveLength(2);
  });
});
