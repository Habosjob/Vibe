from pathlib import Path

from src.pipeline import Pipeline


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    Pipeline(root).run()
