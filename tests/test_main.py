from pathlib import Path

from app.main import main
import os


def test_main(capsys, tmp_path: Path):
    os.chdir(tmp_path)

    main()

    captured = capsys.readouterr()

    assert "Hyundai" in captured.out
    assert "Nissan" in captured.out

    assert "IONIQ 5" in captured.out
