from smart_water.utils.logger import get_logger


def test_logger():
    logger = get_logger("test")

    assert logger.name == "test"
    assert logger.level == 20