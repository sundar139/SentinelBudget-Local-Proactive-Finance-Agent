from pathlib import Path


def test_ui_uses_single_entrypoint_routing() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    ui_dir = repo_root / "ui"

    assert (ui_dir / "app.py").is_file()
    assert (ui_dir / "views").is_dir()
    assert not (ui_dir / "pages").exists()
