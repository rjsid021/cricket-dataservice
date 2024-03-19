import os

from log.log import get_logger

if __name__ == "__main__":
    logger = get_logger("env_file", "env_file")
    logger.error("started creating env file")
    print("started creating file")
    with open(".env", "w") as env_file:
        for key, value in os.environ.items():
            if key in ["DB_PORT", "FTP_PORT", "APP_PORT"]:
                env_file.write(f'{key}={value}\n')
            else:
                env_file.write(f'{key}="{value}"\n')
