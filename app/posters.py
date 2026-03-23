from pathlib import Path
from urllib.parse import quote

POSTER_NAMES = [
    "poster.jpg", "poster.jpeg", "poster.png", "poster.webp",
    "folder.jpg", "folder.jpeg", "folder.png", "folder.webp",
    "cover.jpg", "cover.jpeg", "cover.png", "cover.webp",
    "movie.jpg", "movie.png",
    "season-poster.jpg", "season-poster.png",
]

def local_image_url(path: Path) -> str:
    return f"/api/local_image?path={quote(str(path), safe='')}"

def find_local_poster(path: Path):
    if not path:
        return ""
    candidates = []
    if path.is_file():
        path = path.parent
    if path.exists():
        for name in POSTER_NAMES:
            candidates.append(path / name)
    # for season folder fallback to parent show folder
    if path.exists() and path.parent.exists():
        for name in POSTER_NAMES:
            candidates.append(path.parent / name)
    for c in candidates:
        try:
            if c.exists() and c.is_file():
                return str(c)
        except Exception:
            pass
    return ""

def show_poster(tv_root: str, show_name: str) -> str:
    p = find_local_poster(Path(tv_root) / show_name)
    return local_image_url(Path(p)) if p else ""

def movie_poster(movie_root: str, movie_name: str) -> str:
    p = find_local_poster(Path(movie_root) / movie_name)
    return local_image_url(Path(p)) if p else ""

def target_poster(target_path: str, media_type: str, target_kind: str = "") -> str:
    p = find_local_poster(Path(target_path))
    if p:
        return local_image_url(Path(p))
    # youtube single-file target fallback to sidecar thumbnail in same folder
    tp = Path(target_path)
    if media_type == "youtube":
        folder = tp.parent if tp.is_file() else tp
        stem = tp.stem if tp.is_file() else None
        if folder.exists():
            exts = [".jpg", ".jpeg", ".png", ".webp"]
            names = []
            if stem:
                for ext in exts:
                    names.extend([stem + ext, stem + ".thumb" + ext, "thumb" + ext, "thumbnail" + ext])
            else:
                for ext in exts:
                    names.extend(["thumb" + ext, "thumbnail" + ext])
            for n in names:
                c = folder / n
                if c.exists() and c.is_file():
                    return local_image_url(c)
    return ""
