package com.feed.fanout.service;

import com.feed.fanout.domain.ActivityRepository;
import com.feed.fanout.domain.ColdTierClient;
import com.feed.fanout.domain.HotInboxRepository;
import com.feed.fanout.domain.PostCreatedEvent;
import com.feed.fanout.domain.SocialGraphRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.*;

class FanoutServiceTest {

    private SocialGraphRepository socialGraph;
    private ActivityRepository activity;
    private HotInboxRepository hotInbox;
    private ColdTierClient coldTier;
    private FanoutService service;

    @BeforeEach
    void setUp() {
        socialGraph = mock(SocialGraphRepository.class);
        activity = mock(ActivityRepository.class);
        hotInbox = mock(HotInboxRepository.class);
        coldTier = mock(ColdTierClient.class);
        service = new FanoutService(socialGraph, activity, hotInbox, coldTier, 800, 7);
    }

    private PostCreatedEvent event(long postId, long authorId) {
        PostCreatedEvent e = new PostCreatedEvent();
        e.postId = String.valueOf(postId);
        e.userId = String.valueOf(authorId);
        e.mediaUrl = "https://example.com/a.jpg";
        e.mediaType = 1;
        e.caption = "hello";
        e.createdAt = "2024-01-01T00:00:00Z";
        return e;
    }

    @Test
    void celebrityAuthor_goesToOutboxOnly_neverQueriesFollowers() {
        when(socialGraph.isCelebrity(42L)).thenReturn(true);

        service.handle(event(1L, 42L));

        verify(hotInbox).pushToCelebrityOutbox(eq(42L), eq(1L), anyLong());
        verify(socialGraph, never()).getFollowers(anyLong());
        verifyNoInteractions(activity, coldTier);
    }

    @Test
    void nonCelebrityAuthor_pushesToActiveFollowersHotInbox() {
        when(socialGraph.isCelebrity(2L)).thenReturn(false);
        when(socialGraph.getFollowers(2L)).thenReturn(List.of(10L, 11L, 12L));
        when(activity.filterActive(anyList(), eq(7))).thenReturn(List.of(10L, 11L));

        service.handle(event(100L, 2L));

        verify(hotInbox).pushToInbox(eq(10L), eq(100L), anyLong(), eq(800));
        verify(hotInbox).pushToInbox(eq(11L), eq(100L), anyLong(), eq(800));
        verify(hotInbox, never()).pushToInbox(eq(12L), anyLong(), anyLong(), anyInt());
        verify(hotInbox, never()).pushToCelebrityOutbox(anyLong(), anyLong(), anyLong());
    }

    @Test
    void nonCelebrityAuthor_sendsDormantFollowersToColdTierInsteadOfDropping() {
        when(socialGraph.isCelebrity(2L)).thenReturn(false);
        when(socialGraph.getFollowers(2L)).thenReturn(List.of(10L, 11L, 12L));
        when(activity.filterActive(anyList(), eq(7))).thenReturn(List.of(10L)); // only 10 is active

        service.handle(event(100L, 2L));

        // 11 and 12 are dormant -- must go to the cold tier, not be dropped.
        verify(coldTier).appendToColdTier(eq(11L), eq(100L), eq(2L));
        verify(coldTier).appendToColdTier(eq(12L), eq(100L), eq(2L));
        verify(coldTier, never()).appendToColdTier(eq(10L), anyLong(), anyLong());
    }

    @Test
    void noFollowers_doesNothing() {
        when(socialGraph.isCelebrity(2L)).thenReturn(false);
        when(socialGraph.getFollowers(2L)).thenReturn(List.of());

        service.handle(event(100L, 2L));

        verifyNoInteractions(hotInbox, coldTier);
        verify(activity, never()).filterActive(anyList(), anyInt());
    }
}
