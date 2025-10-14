import sys

from loguru import logger


def main():
    logger.info("OK: python={}, venv={}", sys.version, sys.prefix)


if __name__ == "__main__":
    main()
