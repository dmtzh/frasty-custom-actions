import os
import shutil

def copy_folder(src, dst, exclude_folders=None):
    if exclude_folders is None:
        exclude_folders = []

    for item in os.listdir(src):
        item_path = os.path.join(src, item)
        if os.path.isdir(item_path):
            if item in exclude_folders:
                print(f"Excluding folder: {item_path}")
                continue
            dst_path = os.path.join(dst, item)
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)
            copy_folder(item_path, dst_path, exclude_folders)
        else:
            shutil.copy(item_path, dst)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Recursively copy a folder into another")
    parser.add_argument("src", help="Source folder path (relative or absolute)")
    parser.add_argument("dst", help="Destination folder path (relative or absolute)")
    parser.add_argument("-e", "--exclude", nargs="+", help="Folder names to exclude")

    args = parser.parse_args()

    # Convert relative paths to absolute paths
    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)

    if not os.path.exists(dst):
        os.makedirs(dst)
    copy_folder(src, dst, args.exclude)