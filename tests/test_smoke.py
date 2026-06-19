"""Smoke test: el paquete se instala y se importa correctamente."""

import dev_trends


def test_package_importable() -> None:
    assert dev_trends.__name__ == "dev_trends"
