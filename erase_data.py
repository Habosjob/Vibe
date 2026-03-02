from pathlib import Path
import shutil

TARGET_DIR_NAMES: tuple[str, ...] = ("db", "cache", "raw", "logs")


def is_subpath(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def safe_remove_path(path: Path) -> tuple[int, int, int]:
    files_deleted = 0
    dirs_deleted = 0
    errors = 0

    try:
        if path.is_file() or path.is_symlink():
            path.unlink()
            files_deleted += 1
        elif path.is_dir():
            shutil.rmtree(path)
            dirs_deleted += 1
    except Exception as exc:
        errors += 1
        print(f"[WARN] Не удалось удалить: {path} | {exc}")

    return files_deleted, dirs_deleted, errors


def clear_directory_contents(directory: Path, project_root: Path) -> tuple[int, int, int]:
    files_deleted = 0
    dirs_deleted = 0
    errors = 0

    if not is_subpath(directory, project_root):
        print(f"[WARN] Пропуск небезопасного пути: {directory}")
        return files_deleted, dirs_deleted, errors + 1

    directory.mkdir(parents=True, exist_ok=True)
    print(f"Очистка папки: {directory}")

    for item in directory.iterdir():
        f_count, d_count, e_count = safe_remove_path(item)
        files_deleted += f_count
        dirs_deleted += d_count
        errors += e_count

    return files_deleted, dirs_deleted, errors


def main() -> None:
    project_root = Path(__file__).resolve().parent

    total_files_deleted = 0
    total_dirs_deleted = 0
    total_errors = 0

    print(f"Корень проекта: {project_root}")
    print("Папки для очистки: db, cache, raw, logs")

    for dir_name in TARGET_DIR_NAMES:
        target_dir = project_root / dir_name
        files_deleted, dirs_deleted, errors = clear_directory_contents(target_dir, project_root)
        total_files_deleted += files_deleted
        total_dirs_deleted += dirs_deleted
        total_errors += errors

    print("---")
    print(f"Удалено файлов: {total_files_deleted}")
    print(f"Удалено папок: {total_dirs_deleted}")
    print(f"Ошибок: {total_errors}")

    if total_errors == 0:
        print("Итог: SUCCESS")
    else:
        print("Итог: WARN")


if __name__ == "__main__":
    main()
