# pip install dtlpy pandas
import os
from datetime import datetime, timezone
import pandas as pd
import dtlpy as dl


if dl.token_expired():
    dl.login()

# ===================== CONFIG =====================
PROJECT_NAME = "Litter Annotation Project"
DATASET_NAME = "ML Training"

OUTPUT_DIR   = "reports"
CSV_BASENAME = "per_image_human_vs_bot"

# Exact bot usernames/emails you use (or set env BOT_IDENTIFIERS="a@b,c@d")
BOT_IDENTIFIERS = {
    *(u.strip().lower() for u in os.getenv("BOT_IDENTIFIERS", "").split(",") if u.strip()),
    # examples:
    # "auto-labeler",
    # "model-bot@company.com",
}

# Heuristic: treat any identity that ends with this domain as a bot
BOT_DOMAIN_SUFFIX = "bot.dataloop.ai"

# Optional: only scan a folder prefix (e.g., "/batch-1/"). Leave None to scan all items.
FOLDER_PREFIX = None

# Upload result CSV back to the dataset?
UPLOAD_TO_DATASET = True
# ===================================================


def iso_to_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def normalize_identity(value):
    """Normalize creator/updatedBy which might be str, dict, or object."""
    if value is None:
        return ""
    for attr in ("email", "name", "username"):
        if hasattr(value, attr):
            v = getattr(value, attr)
            if v:
                return str(v).lower()
    if isinstance(value, dict):
        for k in ("email", "name", "username", "id"):
            if value.get(k):
                return str(value[k]).lower()
    try:
        return str(value).lower()
    except Exception:
        return ""


def is_bot(identity: str) -> bool:
    if not identity:
        return False    # unknown -> don’t count as bot
    return identity in BOT_IDENTIFIERS or (BOT_DOMAIN_SUFFIX and identity.endswith(BOT_DOMAIN_SUFFIX))


def classify_annotation(ann) -> dict:
    """
    Returns booleans:
      - bot_created: annotation creator is a bot
      - human_created: annotation creator is a human
      - human_adjusted: bot-created then updated later by a human (updatedAt > createdAt)
    """
    creator = normalize_identity(getattr(ann, "creator", None))
    updated_by = normalize_identity(getattr(ann, "updatedBy", None))
    created_at = iso_to_dt(getattr(ann, "createdAt", None))
    updated_at = iso_to_dt(getattr(ann, "updatedAt", None))

    creator_is_bot = is_bot(creator)
    updated_by_is_bot = is_bot(updated_by)

    bot_created = creator_is_bot
    human_created = (not creator_is_bot) and bool(creator)

    human_adjusted = (
        bot_created
        and updated_by
        and not updated_by_is_bot
        and created_at is not None
        and updated_at is not None
        and updated_at > created_at
    )

    return {"bot_created": bot_created, "human_created": human_created, "human_adjusted": human_adjusted}


def iter_items(dataset: dl.Dataset, folder_prefix: str | None = None, page_size: int = 100):
    """Version-safe items iterator (no context manager)."""
    filters = dl.Filters(resource=dl.FiltersResource.ITEM)
    filters.page_size = page_size
    if folder_prefix:
        filters.add(field="filename", operator=dl.FiltersOperations.STARTSWITH, values=folder_prefix)

    try:
        pages = dataset.items.list(filters=filters)
    except dl.exceptions.BadRequest as e:
        dl.logger.warning(f"Filters failed with 400 ({e}). Retrying without filters...")
        pages = dataset.items.list()

    # Newer dtlpy exposes .all(); else pages is already iterable
    all_fn = getattr(pages, "all", None)
    return all_fn() if callable(all_fn) else pages


def iter_annotations(item: dl.Item, page_size: int = 100):
    """Version-safe annotations iterator: collection is directly iterable; no .all()."""
    coll = item.annotations.list(page_size=page_size)
    all_fn = getattr(coll, "all", None)
    return all_fn() if callable(all_fn) else coll


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # safe timestamp for filenames (no colons)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    csv_path = os.path.join(OUTPUT_DIR, f"{CSV_BASENAME}_{ts}.csv")

    # Connect
    project = dl.projects.get(project_name=PROJECT_NAME)
    dataset = project.datasets.get(dataset_name=DATASET_NAME)

    rows = []
    totals = {"human_created": 0, "bot_created": 0, "human_adjusted_of_bot": 0, "items": 0}

    for item in iter_items(dataset, folder_prefix=FOLDER_PREFIX, page_size=100):
        c_human = 0
        c_bot = 0
        c_human_adj = 0

        for ann in iter_annotations(item, page_size=200):
            cls = classify_annotation(ann)
            if cls["human_created"]:
                c_human += 1
            if cls["bot_created"]:
                c_bot += 1
            if cls["human_adjusted"]:
                c_human_adj += 1

        rows.append({
            "item_id": item.id,
            "filename": item.filename,
            "human_created_count": c_human,
            "bot_created_count": c_bot,
            "bot_created_then_human_adjusted": c_human_adj,
            "total_annotations": c_human + c_bot
        })

        totals["human_created"] += c_human
        totals["bot_created"] += c_bot
        totals["human_adjusted_of_bot"] += c_human_adj
        totals["items"] += 1

    # Save CSV
    df = pd.DataFrame(rows).sort_values(["filename"])
    if df.empty:
        print("No items found (check dataset name or folder filter).")
        return

    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV written: {csv_path}")

    # Summary to console
    print("----- Summary (dataset-wide) -----")
    print(f"Items scanned:                          {totals['items']}")
    print(f"Annotations created by HUMANS:          {totals['human_created']}")
    print(f"Annotations created by BOT/MODEL:       {totals['bot_created']}")
    print(f"Bot-created but later HUMAN-adjusted:   {totals['human_adjusted_of_bot']}")
    all_anns = totals["human_created"] + totals["bot_created"]
    if all_anns:
        print(f"Human share of creations:               {totals['human_created']/all_anns:.2%}")
        print(f"Bot share of creations:                 {totals['bot_created']/all_anns:.2%}")

    # Optional: upload CSV to the dataset under /reports/
    if UPLOAD_TO_DATASET:
        try:
            remote_dir = f"/reports/{CSV_BASENAME}/{ts}"
            dataset.items.upload(local_path=csv_path, remote_path=remote_dir + "/")
            print(f"Uploaded to dataset at: {remote_dir}/")
        except Exception as e:
            print(f"Upload skipped/failed: {e}")


if __name__ == "__main__":
    main()