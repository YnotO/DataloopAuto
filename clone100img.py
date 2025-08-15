# save as copy_100_items_between_datasets.py
import dtlpy as dl


if dl.token_expired():
    dl.login()

# Prompt user for details
project_name = input("Project name: ").strip()
src_dataset_name = input("Source dataset name: ").strip()
dst_dataset_name = input("Destination dataset name: ").strip()

# Connect to project and datasets
project = dl.projects.get(project_name=project_name)
src = project.datasets.get(dataset_name=src_dataset_name)
dst = project.datasets.get(dataset_name=dst_dataset_name)

# Filter for items (adjust if needed)
filters = dl.Filters()
filters.page_size = 100  # We only need 100

# Counter
copied = 0

# Loop through items in the source dataset and copy them
for item in src.items.list(filters=filters).all():
    src.items.clone(
        item_id=item.id,
        dst_dataset_id=dst.id,
        remote_filepath=item.filename,   # Keep same folder structure
        with_annotations=True,           # Include annotations
        with_metadata=True                # Include metadata
    )
    copied += 1
    if copied >= 100:
        break

print(f"Copied {copied} items from '{src_dataset_name}' to '{dst_dataset_name}' (originals kept).")
