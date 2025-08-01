import dtlpy as dl
import random

# Authenticate
if dl.token_expired():
    dl.login()

# Settings
PROJECT_NAME = 'your-project-name'
DATASET_NAME = 'your-dataset-name'
LABELERS = ['user1@yourcompany.com', 'user2@...', 'user3@...', 'user10@...']
FOLDERS_PER_LABELER = 5

# Get project and dataset
project = dl.projects.get(project_name=PROJECT_NAME)
dataset = project.datasets.get(dataset_name=DATASET_NAME)

# Get all folders (directories at root level)
folders = dataset.items.list(filters=dl.Filters(resource=dl.FiltersResource.ITEM, field='type', values='dir')).all()

# Shuffle folders to randomize distribution
random.shuffle(folders)

# Distribute folders across labelers
folder_groups = [folders[i::len(LABELERS)] for i in range(len(LABELERS))]

# Assign folders as tasks
for i, labeler in enumerate(LABELERS):
    for folder in folder_groups[i][:FOLDERS_PER_LABELER]:
        print(f"Assigning folder '{folder.name}' to {labeler}")
        
        task = dataset.tasks.create(
            task_name=folder.name,
            assignee_ids=[labeler],
            filters=dl.Filters(field='dir', values=folder.dir),
            status='annotation'
        )
