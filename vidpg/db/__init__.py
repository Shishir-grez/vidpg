"""PostgreSQL frame-plane primitives for VidPG P3."""

from .buckets import (
    active_bucket,
    bucket_table,
    next_bucket,
    previous_bucket,
)
from .connection import (
    PgConnection,
    PgPool,
    open_dedicated_listener,
    open_maintenance_connection,
    open_pool,
)
from .fetcher import (
    choose_newer,
    fetch_bucket_candidate,
    fetch_latest_after,
)
from .notifications import (
    DirtyState,
    FrameSignal,
    NotificationParseError,
    NotificationStream,
    listen_frames,
    mark_dirty,
    parse_frame_notification,
    rescan_latest_after_listen,
)
from .rotation import (
    CleanupResult,
    RotationResult,
    publish_next_active,
    rotation_loop,
    run_rotation_once,
    truncate_next_bucket,
)
from .schema import (
    SchemaContractError,
    SchemaReport,
    apply_schema,
    assert_schema_matches_contract,
)
from .stats import (
    PgDelta,
    PgSnapshot,
    RelationSize,
    RelationSizes,
    capture_pg_stats,
    diff_pg_stats,
    relation_sizes,
)
from .writer import (
    FrameWriteError,
    InsertReceipt,
    PreparedStatement,
    insert_and_notify,
    insert_frame,
    prepare_insert,
)

__all__ = [
    "CleanupResult",
    "DirtyState",
    "FrameSignal",
    "FrameWriteError",
    "InsertReceipt",
    "NotificationParseError",
    "NotificationStream",
    "PgConnection",
    "PgDelta",
    "PgPool",
    "PgSnapshot",
    "PreparedStatement",
    "RelationSize",
    "RelationSizes",
    "RotationResult",
    "SchemaContractError",
    "SchemaReport",
    "active_bucket",
    "apply_schema",
    "assert_schema_matches_contract",
    "bucket_table",
    "capture_pg_stats",
    "choose_newer",
    "diff_pg_stats",
    "fetch_bucket_candidate",
    "fetch_latest_after",
    "insert_and_notify",
    "insert_frame",
    "listen_frames",
    "mark_dirty",
    "next_bucket",
    "open_dedicated_listener",
    "open_maintenance_connection",
    "open_pool",
    "parse_frame_notification",
    "prepare_insert",
    "previous_bucket",
    "publish_next_active",
    "relation_sizes",
    "rescan_latest_after_listen",
    "rotation_loop",
    "run_rotation_once",
    "truncate_next_bucket",
]
