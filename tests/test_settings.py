from smart_water.config.settings import settings


def test_settings():
    assert settings.app_env == "development"
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5432