import sys
import pytest
from run_cli import main


def test_cli_valid_number(monkeypatch, capsys):

    monkeypatch.setattr(sys, "argv", ["run_cli.py", "25"])

    main()

    captured = capsys.readouterr()

    assert "Número extraído: 25" in captured.out
    assert "Número faltante calculado: 25" in captured.out


def test_cli_invalid_argument(monkeypatch):

    monkeypatch.setattr(sys, "argv", ["run_cli.py", "hola"])

    with pytest.raises(SystemExit):
        main()


def test_cli_out_of_range(monkeypatch):

    monkeypatch.setattr(sys, "argv", ["run_cli.py", "200"])

    with pytest.raises(SystemExit):
        main()