from datetime import datetime
from typing import Annotated, Literal, Union

from pydantic import Field, model_validator

from .base import MetadataModel, StrictModel


class ReactionInfo(StrictModel):
    up_count: int = Field(default=0, ge=0)
    down_count: int = Field(default=0, ge=0)
    score: int = 0

    @model_validator(mode="after")
    def score_matches(self):
        self.score = self.up_count - self.down_count
        return self


class AuthorInfo(StrictModel):
    nickname: str
    member_srl: str | None = None
    level: str | None = None
    profile_url: str | None = None


class CategoryInfo(StrictModel):
    category_id: str | None = None
    name: str | None = None
    url: str | None = None


class TextBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["text"] = "text"
    text: str


class ImageBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["image"] = "image"
    url: str
    file_srl: str | None = None
    alt: str | None = None
    width: int | None = None
    height: int | None = None
    download_url: str | None = None
    download: bool | None = None
    file_path: str | None = None
    object_key: str | None = None


class FmkVideoBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["fmk_video"] = "fmk_video"
    video_url: str
    poster_url: str | None = None
    file_srl: str | None = None
    download_url: str | None = None
    name: str | None = None
    upload_date: datetime | None = None
    description: str | None = None
    width: int | None = None
    height: int | None = None
    download: bool | None = None
    file_path: str | None = None
    object_key: str | None = None


class YouTubeBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["youtube"] = "youtube"
    video_id: str
    url: str
    embed_url: str
    title: str | None = None
    start_seconds: int = 0


class XBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["x"] = "x"
    username: str | None = None
    status_id: str | None = None
    tweet_id: str | None = None
    url: str
    embed_url: str | None = None
    text: str | None = None


class LinkBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["link"] = "link"
    text: str
    url: str


class AttachmentBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["attachment"] = "attachment"
    file_name: str
    url: str
    file_srl: str | None = None


class UnknownEmbedBlock(StrictModel):
    order: int = Field(ge=0)
    type: Literal["unknown_embed"] = "unknown_embed"
    provider: str = "iframe"
    url: str | None = None


ContentBlock = Annotated[Union[TextBlock, ImageBlock, FmkVideoBlock, YouTubeBlock, XBlock, LinkBlock,
    AttachmentBlock, UnknownEmbedBlock], Field(discriminator="type")]


class ContentInfo(StrictModel):
    text: str = ""
    blocks: list[ContentBlock] = Field(default_factory=list)


class AttachmentInfo(StrictModel):
    file_name: str
    url: str
    file_srl: str | None = None
    sid: str | None = None
    mid: str | None = None
    size_text: str | None = None
    download_text: str | None = None


class MediaItem(StrictModel):
    media_id: str
    media_type: Literal["image", "fmk_video", "youtube", "x", "link"]
    source_block_order: int = Field(ge=0)
    url: str
    file_path: str | None = None
    object_key: str | None = None
    description: str | None = None
    provider: str | None = None
    embed_url: str | None = None
    file_srl: str | None = None


class CommentInfo(StrictModel):
    comment_srl: int | None = None
    comment_id_raw: str | None = None
    source_dom_id: str | None = None
    parent_comment_srl: int | None = None
    parent_comment_id_raw: str | None = None
    depth: int = Field(default=0, ge=0)
    source_order: int = Field(ge=0)
    is_best: bool = False
    badges: list[str] = Field(default_factory=list)
    author: AuthorInfo
    created_at_raw: str | None = None
    created_at: datetime | None = None
    content: str
    reactions: ReactionInfo = Field(default_factory=ReactionInfo)
    permalink: str | None = None


class PostResult(MetadataModel):
    schema_version: Literal["fmkorea.post.v1"] = "fmkorea.post.v1"
    scraped_at: datetime
    site_id: Literal["fmkorea"] = "fmkorea"
    board_id: str
    board_name: str | None = None
    document_srl: int = Field(ge=1)
    document_id_raw: str
    canonical_url: str
    category: CategoryInfo = Field(default_factory=CategoryInfo)
    title: str
    author: AuthorInfo
    created_at: datetime | None = None
    created_at_raw: str | None = None
    views: int = Field(default=0, ge=0)
    reactions: ReactionInfo = Field(default_factory=ReactionInfo)
    comment_count: int = Field(default=0, ge=0)
    content: ContentInfo = Field(default_factory=ContentInfo)
    attachments: list[AttachmentInfo] = Field(default_factory=list)
    media: list[MediaItem] = Field(default_factory=list)
    comments: list[CommentInfo] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_id(self):
        if str(self.document_srl) != self.document_id_raw:
            raise ValueError("document_srl and document_id_raw disagree")
        return self
