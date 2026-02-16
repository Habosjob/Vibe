import re
from urllib.parse import urlparse

import requests


DATE_PATTERN = re.compile(r"(?<!\d)(\d{8})(?!\d)")


def get_repo_info(repo_url):
    """Извлекает owner, repo и ветку из URL GitHub."""
    parsed = urlparse(repo_url)
    path_parts = parsed.path.strip('/').split('/')

    if len(path_parts) < 2:
        raise ValueError("Некорректный URL репозитория GitHub.")

    owner = path_parts[0]
    repo = path_parts[1]
    branch = "main"  # ветка по умолчанию

    # Если в URL есть /tree/<ветка>, извлекаем её
    if len(path_parts) >= 4 and path_parts[2] == "tree":
        branch = path_parts[3]

    return owner, repo, branch


def list_files_via_api(owner, repo, branch):
    """Получает список файлов через GitHub API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Ошибка API GitHub: {response.status_code} — {response.text}")

    tree = response.json().get("tree", [])
    files = [item["path"] for item in tree if item["type"] == "blob"]
    return files


def sort_files_by_depth(file_paths):
    """Сортирует файлы по уровню вложенности и алфавиту."""
    files_by_depth = {}

    for path in file_paths:
        depth = path.count('/')
        if depth not in files_by_depth:
            files_by_depth[depth] = []
        files_by_depth[depth].append(path)

    sorted_files = []
    for depth in sorted(files_by_depth.keys()):
        files_by_depth[depth].sort()
        sorted_files.extend(files_by_depth[depth])

    return sorted_files


def get_directory_structure(file_paths):
    """Возвращает структуру директорий для разделителей."""
    directories = set()

    for path in file_paths:
        parts = path.split('/')
        for i in range(1, len(parts)):
            dir_path = '/'.join(parts[:i])
            directories.add(dir_path)

    return sorted(directories)


def generate_raw_urls(owner, repo, branch, file_paths):
    """Создаёт RAW-ссылки для списка файлов с группировкой по директориям."""
    base_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}"
    sorted_files = sort_files_by_depth(file_paths)

    directories = get_directory_structure(sorted_files)

    files_by_dir = {}
    for path in sorted_files:
        dir_name = '/'.join(path.split('/')[:-1]) if '/' in path else ""
        if dir_name not in files_by_dir:
            files_by_dir[dir_name] = []
        files_by_dir[dir_name].append(path)

    raw_urls = []

    if "" in files_by_dir:
        if "" in directories:
            directories.remove("")
        raw_urls.append(f"\n{'='*60}")
        raw_urls.append("КОРНЕВАЯ ДИРЕКТОРИЯ:")
        raw_urls.append(f"{'='*60}\n")
        for path in files_by_dir[""]:
            raw_urls.append(f"{base_url}/{path}")

    for dir_path in sorted(directories, key=lambda x: (x.count('/'), x)):
        if dir_path in files_by_dir:
            depth = dir_path.count('/')
            indent = "  " * depth
            separator = "-" * (60 - depth * 2)

            raw_urls.append(f"\n{indent}{separator}")
            raw_urls.append(f"{indent}Директория: {dir_path}/")
            raw_urls.append(f"{indent}{separator}\n")

            for path in files_by_dir[dir_path]:
                raw_urls.append(f"{base_url}/{path}")

    return raw_urls


def extract_date(path):
    """Извлекает дату YYYYMMDD из пути/имени файла."""
    match = DATE_PATTERN.search(path)
    return match.group(1) if match else None


def select_latest_by_date(paths, date_limit, max_files=None):
    """Оставляет файлы только для последних N дат из набора путей."""
    dated_paths = []
    for path in paths:
        date = extract_date(path)
        if date:
            dated_paths.append((date, path))

    latest_dates = sorted({date for date, _ in dated_paths}, reverse=True)[:date_limit]
    latest_set = set(latest_dates)

    selected = [path for date, path in sorted(dated_paths, key=lambda x: (x[0], x[1]), reverse=True) if date in latest_set]
    if max_files is not None:
        return selected[:max_files]
    return selected


def generate_latest_raw_urls(owner, repo, branch, file_paths):
    """Создаёт компактный манифест ссылок только на актуальные диагностические артефакты."""
    base_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}"
    latest_urls = []

    root_files = sorted(
        [
            "README.md",
            "generate_raw_links.py",
            "pytest.ini",
            "raw_links.txt",
            "raw_links_latest.txt",
            "requirements.txt",
        ]
    )

    latest_urls.append(f"\n{'=' * 60}")
    latest_urls.append("КОРНЕВЫЕ ФАЙЛЫ:")
    latest_urls.append(f"{'=' * 60}\n")
    for path in root_files:
        latest_urls.append(f"{base_url}/{path}")

    probe_logs = [
        path
        for path in file_paths
        if path.startswith("logs/moex_endpoints_probe_") and path.endswith(".log")
    ]
    latest_probe_logs = sorted(select_latest_by_date(probe_logs, date_limit=7, max_files=20))

    latest_urls.append(f"\n{'=' * 60}")
    latest_urls.append("ПОСЛЕДНИЕ LOG-ФАЙЛЫ PROBE:")
    latest_urls.append(f"{'=' * 60}\n")
    for path in latest_probe_logs:
        latest_urls.append(f"{base_url}/{path}")

    cache_probe_json = [
        path
        for path in file_paths
        if path.startswith("data/cache/moex_iss/endpoint_probe/") and path.endswith(".json")
    ]
    latest_cache_probe_json = sorted(select_latest_by_date(cache_probe_json, date_limit=3, max_files=30))

    latest_urls.append(f"\n{'=' * 60}")
    latest_urls.append("ПОСЛЕДНИЕ CACHE JSON (endpoint_probe):")
    latest_urls.append(f"{'=' * 60}\n")
    for path in latest_cache_probe_json:
        latest_urls.append(f"{base_url}/{path}")

    curated_probe_xlsx = [
        path
        for path in file_paths
        if path.startswith("data/curated/moex/endpoints_probe/") and path.endswith(".xlsx")
    ]
    latest_curated_probe_xlsx = sorted(select_latest_by_date(curated_probe_xlsx, date_limit=3, max_files=20))

    latest_urls.append(f"\n{'=' * 60}")
    latest_urls.append("ПОСЛЕДНИЕ CURATED XLSX (endpoints_probe):")
    latest_urls.append(f"{'=' * 60}\n")
    for path in latest_curated_probe_xlsx:
        latest_urls.append(f"{base_url}/{path}")

    return latest_urls


def main():
    repo_url = "https://github.com/Habosjob/Vibe"  # Ваша ссылка

    try:
        owner, repo, branch = get_repo_info(repo_url)
        print(f"Обрабатываем репозиторий: {owner}/{repo} (ветка: {branch})")

        files = list_files_via_api(owner, repo, branch)
        print(f"Найдено файлов: {len(files)}")

        sorted_files = sort_files_by_depth(files)
        print("Файлы отсортированы по уровням вложенности")

        raw_urls = generate_raw_urls(owner, repo, branch, sorted_files)

        with open("raw_links.txt", "w", encoding="utf-8") as f:
            for url in raw_urls:
                f.write(url + "\n")

        latest_raw_urls = generate_latest_raw_urls(owner, repo, branch, sorted_files)
        with open("raw_links_latest.txt", "w", encoding="utf-8") as f:
            for url in latest_raw_urls:
                f.write(url + "\n")

        print("Ссылки успешно сохранены в файл: raw_links.txt")
        print("Короткий список сохранён в файл: raw_links_latest.txt")
        print("Файлы сгруппированы по директориям с разделителями")

    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()
