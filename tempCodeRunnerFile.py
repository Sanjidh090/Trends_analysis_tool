import os

def print_tree(start_path, prefix=""):
    """Recursively prints directory structure in ASCII format."""
    try:
        items = sorted(os.listdir(start_path))
    except FileNotFoundError:
        print(f"Error: Path '{start_path}' not found.")
        return
    except PermissionError:
        print(f"Error: Permission denied for '{start_path}'.")
        return

    for index, item in enumerate(items):
        path = os.path.join(start_path, item)
        connector = "└── " if index == len(items) - 1 else "├── "
        print(prefix + connector + item)
        if os.path.isdir(path):
            extension = "    " if index == len(items) - 1 else "│   "
            print_tree(path, prefix + extension)

# Example usage:
if __name__ == "__main__":
    dataset_path = r"C:\Users\sanji\Downloads\files"  # Change to your dataset folder
    print(f"Dataset structure for: {dataset_path}")
    print_tree(dataset_path)