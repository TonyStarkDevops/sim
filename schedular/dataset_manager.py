FAMOUS_DATASETS = {
    'coco',
    'ImageNet',
    'MIT_Places'
    # todo: to be completed
}


def is_dataset_famous(dataset_name: str) -> bool:
    if dataset_name in FAMOUS_DATASETS:
        return True
    return False
