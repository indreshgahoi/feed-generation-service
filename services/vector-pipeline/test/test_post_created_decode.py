"""Round-trip test for main.decode_post_created against a hand-built
schemas/fbs/post_created.fbs FlatBuffer -- the first automated test this
service has (it previously had none at all), and the first to actually
exercise the wire format rather than a mock. See doc/DESIGN.md.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "genfbs"))

import flatbuffers  # noqa: E402
from feed.events.MediaType import MediaType  # noqa: E402
from feed.events import PostCreatedEvent as fb_event  # noqa: E402

from main import decode_post_created  # noqa: E402


def build_post_created_fb(post_id, user_id, media_url, media_type, caption, created_at):
    builder = flatbuffers.Builder(0)
    media_url_off = builder.CreateString(media_url)
    caption_off = builder.CreateString(caption)

    fb_event.PostCreatedEventStart(builder)
    fb_event.PostCreatedEventAddPostId(builder, post_id)
    fb_event.PostCreatedEventAddUserId(builder, user_id)
    fb_event.PostCreatedEventAddMediaUrl(builder, media_url_off)
    fb_event.PostCreatedEventAddMediaType(builder, media_type)
    fb_event.PostCreatedEventAddCaption(builder, caption_off)
    fb_event.PostCreatedEventAddCreatedAt(builder, created_at)
    offset = fb_event.PostCreatedEventEnd(builder)

    builder.Finish(offset)
    return bytes(builder.Output())


class DecodePostCreatedTest(unittest.TestCase):
    def test_round_trips_all_fields(self):
        raw = build_post_created_fb(
            post_id=357748213593194496,
            user_id=357748214239117312,
            media_url="https://example.com/img.jpg",
            media_type=MediaType.Image,
            caption="hello @user_3",
            created_at=1735689600000,
        )

        event = decode_post_created(raw)

        self.assertEqual(event["postId"], 357748213593194496)
        self.assertEqual(event["userId"], 357748214239117312)
        self.assertEqual(event["mediaUrl"], "https://example.com/img.jpg")
        self.assertEqual(event["mediaType"], MediaType.Image)
        self.assertEqual(event["caption"], "hello @user_3")
        self.assertEqual(event["createdAt"], 1735689600000)

    def test_empty_caption_decodes_to_empty_string_not_none(self):
        raw = build_post_created_fb(
            post_id=1, user_id=2, media_url="https://example.com/a.jpg",
            media_type=MediaType.Video, caption="", created_at=0,
        )

        event = decode_post_created(raw)

        self.assertEqual(event["caption"], "")


if __name__ == "__main__":
    unittest.main()
