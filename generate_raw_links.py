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

def sort_files_by_depth(file_paths):
    """Сортирует файлы по уровню вложенности и алфавиту."""
    # Группируем файлы по уровню вложенности
    files_by_depth = {}
    
    for path in file_paths:
        # Подсчитываем уровень вложенности по количеству разделителей
        depth = path.count('/')
        if depth not in files_by_depth:
            files_by_depth[depth] = []
        files_by_depth[depth].append(path)
    
    # Сортируем по глубине и алфавиту внутри каждой группы
    sorted_files = []
    for depth in sorted(files_by_depth.keys()):
        # Сортируем файлы в текущем уровне по алфавиту
        files_by_depth[depth].sort()
        sorted_files.extend(files_by_depth[depth])
    
    return sorted_files

def get_directory_structure(file_paths):
    """Возвращает структуру директорий для разделителей."""
    directories = set()
    
    for path in file_paths:
        # Добавляем все родительские директории
        parts = path.split('/')
        for i in range(1, len(parts)):
            dir_path = '/'.join(parts[:i])
            directories.add(dir_path)
    
    return sorted(directories)

def generate_raw_urls(owner, repo, branch, file_paths):
    """Создаёт RAW-ссылки для списка файлов с группировкой по директориям."""
    base_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}"
    sorted_files = sort_files_by_depth(file_paths)
    
    # Получаем структуру директорий
    directories = get_directory_structure(sorted_files)
    
    # Создаем словарь для группировки файлов по директориям
    files_by_dir = {}
    for path in sorted_files:
        dir_name = '/'.join(path.split('/')[:-1]) if '/' in path else ""
        if dir_name not in files_by_dir:
            files_by_dir[dir_name] = []
        files_by_dir[dir_name].append(path)
    
    # Генерируем ссылки с группировкой
    raw_urls = []
    current_dir = ""
    
    # Обрабатываем корневую директорию
    if "" in files_by_dir:
        if "" in directories:
            directories.remove("")
        raw_urls.append(f"\n{'='*60}")
        raw_urls.append("КОРНЕВАЯ ДИРЕКТОРИЯ:")
        raw_urls.append(f"{'='*60}\n")
        for path in files_by_dir[""]:
            raw_urls.append(f"{base_url}/{path}")
    
    # Обрабатываем остальные директории в порядке вложенности
    for dir_path in sorted(directories, key=lambda x: (x.count('/'), x)):
        if dir_path in files_by_dir:
            # Добавляем разделитель для новой директории
            depth = dir_path.count('/')
            indent = "  " * depth
            separator = "-" * (60 - depth * 2)
            
            raw_urls.append(f"\n{indent}{separator}")
            raw_urls.append(f"{indent}Директория: {dir_path}/")
            raw_urls.append(f"{indent}{separator}\n")
            
            # Добавляем файлы из этой директории
            for path in files_by_dir[dir_path]:
                raw_urls.append(f"{base_url}/{path}")
    
    return raw_urls

def main():
    repo_url = "https://github.com/Habosjob/Vibe"  # Ваша ссылка

    try:
        owner, repo, branch = get_repo_info(repo_url)
        print(f"Обрабатываем репозиторий: {owner}/{repo} (ветка: {branch})")

        files = list_files_via_api(owner, repo, branch)
        print(f"Найдено файлов: {len(files)}")
        
        # Сортируем файлы
        sorted_files = sort_files_by_depth(files)
        print(f"Файлы отсортированы по уровням вложенности")
        
        # Генерируем URL с группировкой
        raw_urls = generate_raw_urls(owner, repo, branch, sorted_files)

        # Сохраняем в файл
        with open("raw_links.txt", "w", encoding="utf-8") as f:
            for url in raw_urls:
                f.write(url + "\n")

        print("Ссылки успешно сохранены в файл: raw_links.txt")
        print("Файлы сгруппированы по директориям с разделителями")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()