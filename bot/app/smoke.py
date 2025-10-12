from loguru import logger
import sys


def main():
    logger.info("OK: python={}, venv={}", sys.version, sys.prefix)


if __name__ == "__main__":
    main()
