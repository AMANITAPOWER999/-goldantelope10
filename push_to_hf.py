"""Пуш изменений на Hugging Face Space poweramanita/goldantelopeasia_bot"""
import os
from huggingface_hub import HfApi

TOKEN = os.environ.get('HF_TOKEN', '')
if not TOKEN:
    try:
        with open('.env.hf') as f:
            for line in f:
                if line.startswith('HF_TOKEN='):
                    TOKEN = line.strip().split('=', 1)[1]
    except Exception:
        pass

if not TOKEN:
    print("ERROR: HF_TOKEN не найден")
    exit(1)

REPO = "poweramanita/goldantelopeasia_bot"
api = HfApi(token=TOKEN)

FILES = [
    ("/tmp/hf_push/app.py", "app.py"),
    ("templates/dashboard.html", "templates/dashboard.html"),
    ("listings_vietnam.json", "listings_vietnam.json"),
    ("listings_data.json", "listings_data.json"),
    ("vietnamparsing_parser.py", "vietnamparsing_parser.py"),
    ("file_id_index.json", "file_id_index.json"),
    ("banner_config.json", "banner_config.json"),
]

for local_path, repo_path in FILES:
    if not os.path.exists(local_path):
        print(f"  ~ пропуск (нет файла): {local_path}")
        continue
    size = os.path.getsize(local_path) / 1024 / 1024
    print(f"Загружаю {local_path} -> {repo_path} ({size:.1f} MB)...")
    try:
        api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=repo_path,
            repo_id=REPO,
            repo_type="space",
            commit_message=f"Update {repo_path}",
        )
        print(f"  ✓ {repo_path}")
    except Exception as e:
        print(f"  ✗ {repo_path}: {e}")

print("\nГотово! Space: https://huggingface.co/spaces/poweramanita/goldantelopeasia_bot")
