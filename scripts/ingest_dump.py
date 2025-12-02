# scripts/download_dump.py
import requests
from pathlib import Path
from tqdm import tqdm
import time
import tarfile
import argparse

def download_dump(url: str, output_file: Path) -> Path:
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    url = "https://static.crates.io/db-dump.tar.gz"
    output_file = data_dir / "db-dump.tar.gz"
    
    print(f"Downloading: {url}")
    print(f"Output: {output_file.absolute()}\n")
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    start_time = time.time()
    
    # Progress bar with tqdm
    with open(output_file, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
                pbar.update(len(chunk))
    
    elapsed = time.time() - start_time
    size_gb = output_file.stat().st_size / (1024**3)
    speed = (output_file.stat().st_size / elapsed) / (1024**2)
    
    print(f"\n✓ Download complete!")
    print(f"  Size: {size_gb:.2f} GB")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Avg Speed: {speed:.2f} MB/s")
    
    return output_file

def extract_dump(dump_file: Path, extract_dir: Path) -> Path:
    print(f"Extracting {dump_file}...")
    print(f"Output: {extract_dir}")
    
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    with tarfile.open(dump_file, 'r:gz') as tar:
        tar.extractall(path=extract_dir)
    
    print("\n✓ Extraction complete!")
    print(f"\nFiles extracted:")
    for file in sorted(extract_dir.rglob("*.csv")):
        size_mb = file.stat().st_size / (1024**2)
        print(f"  {file.name}: {size_mb:.1f} MB")
    
    return extract_dir

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download, only extract existing dump"
    )
    parser.add_argument(
        "--dump-file",
        type=Path,
        default=Path("data/raw/db-dump.tar.gz")
    )
    parser.add_argument(
        "--extract-dir",
        type=Path,
        default=Path("data/raw/extracted")
    )
    
    args = parser.parse_args()
    
    url = "https://static.crates.io/db-dump.tar.gz"
    
    # Download unless skipped
    if not args.skip_download:
        download_dump(url, args.dump_file)
    
    # Always extract
    extract_dump(args.dump_file, args.extract_dir)
