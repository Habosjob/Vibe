import requests
from urllib.parse import urlparse

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

def generate_raw_urls(owner, repo, branch, file_paths):
    """Создаёт RAW‑ссылки для списка файлов."""
    base_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}"
    return [f"{base_url}/{path}" for path in file_paths]

def main():
    repo_url = "https://github.com/Habosjob/Vibe"  # Ваша ссылка

    try:
        owner, repo, branch = get_repo_info(repo_url)
        print(f"Обрабатываем репозиторий: {owner}/{repo} (ветка: {branch})")

        files = list_files_via_api(owner, repo, branch)
        print(f"Найдено файлов: {len(files)}")

        raw_urls = generate_raw_urls(owner, repo, branch, files)

        # Сохраняем в файл
        with open("raw_links.txt", "w", encoding="utf-8") as f:
            for url in raw_urls:
                f.write(url + "\n")

        print("Ссылки успешно сохранены в файл: raw_links.txt")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()
