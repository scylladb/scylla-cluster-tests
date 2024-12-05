import os
import glob


def list_files_and_sizes(base_path, output_file):
    """
    List all files and their sizes in a directory pattern, appending the results to a file.

    :param base_path: Base directory pattern (e.g., /var/lib/scylla/data/scylla_bench/test-*/).
    :param output_file: Path to the output file.
    """
    try:
        with open(output_file, 'w') as f:
            # Expand and iterate over all matching directories
            for dir_path in glob.glob(base_path):
                # Check if it's a valid directory
                if not os.path.isdir(dir_path):
                    continue

                # Iterate over all files in the directory
                for root, _, files in os.walk(dir_path):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        try:
                            file_size_kb = os.path.getsize(file_path) // 1024  # Convert bytes to KB
                            f.write(f"{file_path}: {file_size_kb} KB\n")
                        except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
                            print(f"Failed to get size for {file_path}: {e}")
    except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
        print(f"Failed to list files: {e}")
        return False

    return True


if __name__ == "__main__":
    base_directory_pattern = "/var/lib/scylla/data/scylla_bench/test-*"
    output_file_path = "/tmp/files_and_sizes.txt"  # Specify your output file path here

    success = list_files_and_sizes(base_directory_pattern, output_file_path)

    if success:
        print(f"File details written to: {output_file_path}")
    else:
        print("Failed to list files.")
