import psycopg2

from smart_water.config.settings import settings
from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def main():
    logger.info("Connecting to PostgreSQL...")

    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )

    cursor = connection.cursor()

    cursor.execute("SELECT version();")

    version = cursor.fetchone()

    print("\n=== POSTGRESQL CONNECTION ===")
    print(version[0])

    cursor.execute("SELECT current_database();")

    database = cursor.fetchone()

    print("\n=== DATABASE ===")
    print(database[0])

    cursor.close()
    connection.close()

    logger.info("PostgreSQL connection successful")


if __name__ == "__main__":
    main()