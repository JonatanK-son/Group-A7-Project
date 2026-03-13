import os
import httpx
from tqdm import tqdm
import argparse
from pathlib import Path

DATA_DIR = Path("data")
# Primary source (may require authentication/token for full size)
DEFAULT_URL = "https://huggingface.co/datasets/kevykibbz/ecommerce-behavior-data-from-multi-category-store_oct-nov_2019/resolve/main/2019-Oct.csv"
SAMPLE_SIZE_MB = 100

def generate_synthetic_data(dest_path: Path, size_mb: int):
    """Generate a synthetic CSV sample that matches the project schema."""
    import random
    import datetime
    
    print(f"Generating {size_mb}MB of synthetic data to {dest_path}...")
    headers = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
    event_types = ["view", "cart", "purchase", "remove_from_cart"]
    brands = ["samsung", "apple", "huawei", "lg", "sony", "unknown"]
    categories = ["electronics.smartphone", "appliances.kitchen.refrigerators", "computers.notebook", "electronics.audio.headphone"]
    
    bytes_written = 0
    target_bytes = size_mb * 1024 * 1024
    
    with open(dest_path, "w", encoding="utf-8") as f:
        f.write(headers)
        bytes_written += len(headers)
        
        while bytes_written < target_bytes:
            now = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
            line = (
                f"{now.strftime('%Y-%m-%d %H:%M:%S')} UTC,"
                f"{random.choice(event_types)},"
                f"{random.randint(1000, 9999999)},"
                f"{random.randint(2000000000000000000, 3000000000000000000)},"
                f"{random.choice(categories)},"
                f"{random.choice(brands)},"
                f"{random.uniform(10.0, 2000.0):.2f},"
                f"{random.randint(100000000, 999999999)},"
                f"{str(datetime.datetime.now().timestamp()) + str(random.random())}\n"
            )
            f.write(line)
            bytes_written += len(line)
    
    print(f"Synthetic data generated successfully.")

def download_file(url: str, dest_path: Path, sample: bool = False):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    headers = {}
    if sample:
        # Download roughly SAMPLE_SIZE_MB + a bit more to handle line truncation
        limit = SAMPLE_SIZE_MB * 1024 * 1024
        headers["Range"] = f"bytes=0-{limit}"
        print(f"Downloading ~{SAMPLE_SIZE_MB}MB sample to {dest_path}...")
    else:
        print(f"Downloading full dataset to {dest_path} (this may take a while)...")

    with httpx.stream("GET", url, headers=headers, follow_redirects=True) as response:
        if response.status_code not in (200, 206):
            print(f"Warning: Failed to download file. Status code: {response.status_code}")
            return False

        total = int(response.headers.get("Content-Length", 0))
        
        with open(dest_path, "wb") as f:
            with tqdm(total=total, unit="B", unit_scale=True, desc=dest_path.name) as pbar:
                for chunk in response.iter_bytes(chunk_size=16384):
                    f.write(chunk)
                    pbar.update(len(chunk))
    
    if sample:
        # Optional: Truncate to the last newline to ensure a valid CSV
        with open(dest_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size > 0:
                f.seek(max(0, size - 1024))
                last_chunk = f.read()
                last_newline = last_chunk.rfind(b"\n")
                if last_newline != -1:
                    truncate_pos = size - (len(last_chunk) - last_newline)
                    f.seek(truncate_pos)
                    f.truncate()
        print(f"Sample downloaded and truncated to last full line.")
    return True

def main():
    parser = argparse.ArgumentParser(description="Download ecommerce behavior data.")
    parser.add_argument("--sample", action="store_true", help=f"Download only a ~{SAMPLE_SIZE_MB}MB sample.")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL to download from.")
    parser.add_argument("--output", default="2019-Oct.csv", help="Output filename in data/ directory.")
    
    args = parser.parse_args()
    
    output_path = DATA_DIR / args.output
    success = download_file(args.url, output_path, sample=args.sample)
    
    if not success:
        print("Falling back to synthetic data generation to ensure pipeline can run.")
        generate_synthetic_data(output_path, SAMPLE_SIZE_MB if args.sample else 500)

if __name__ == "__main__":
    main()
