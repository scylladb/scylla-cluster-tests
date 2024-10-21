import subprocess

import requests
import shutil
import tempfile
from pathlib import Path


def get_latest_tarball_url(package_name: str) -> tuple[str, str]:
    """Fetches the latest tarball URL and version from PyPI for the given package."""
    url = f"https://pypi.org/pypi/{package_name}/json"
    response = requests.get(url)
    data = response.json()
    version = data['info']['version']
    for file_info in data['urls']:
        if file_info['filename'].endswith('.tar.gz'):
            return file_info['url'], version
    raise Exception(f"No .tar.gz file found for {package_name}")


def download_tarball(url: str, dest_folder: Path) -> Path:
    """Downloads the tarball to the destination folder."""
    tarball_path = dest_folder / url.split('/')[-1]
    response = requests.get(url, stream=True)
    with open(tarball_path, 'wb') as f:
        f.write(response.content)
    return tarball_path


def remove_old_files(project_root: Path):
    if (project_root / "argus" / "version").exists():
        shutil.rmtree(project_root / "argus")
    else:
        raise Exception("No version file found. Please make sure you are in the correct directory.")


def extract_core_files(tarball_path: Path, extract_to: Path, version: str) -> None:
    command = [
        'tar',
        '--strip-components=1',
        '-xzf', tarball_path,
        '-C', extract_to,
        f'argus_alm-{version}/argus'
    ]
    subprocess.run(command, check=True)


def create_version_file(extract_to: Path, version: str):
    """Creates a 'version' file with the current version."""
    version_file_path = extract_to / 'version'
    with open(version_file_path, 'w') as f:
        f.write(f"{version}\n")


def create_how_to_update_file(extract_to: Path):
    """Creates 'how_to_update.md' with predefined content."""
    how_to_update_path = extract_to / 'how_to_update.md'
    with open(how_to_update_path, 'w') as f:
        f.write("run `python utils/update_argus_client.py` to update Argus Client\n")


def update_package(package_name: str, project_root: Path):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_folder = Path(temp_dir)
        tarball_url, version = get_latest_tarball_url(package_name)
        tarball_path = download_tarball(tarball_url, temp_folder)
        extract_core_files(tarball_path, project_root, version=version)
        create_version_file(project_root / "argus", version)
        create_how_to_update_file(project_root / "argus")

    print(f"{package_name} updated to version {version}.")


if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    update_package("argus-alm", project_root)
