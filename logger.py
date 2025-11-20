import logging
import os
from logging.handlers import RotatingFileHandler

# Create 'logs' directory if it doesn't exist
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Logger setup
log_file = os.path.join(log_directory, 'app.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Configure the RotatingFileHandler
# This rotates logs when the file reaches 5 MB and keeps the last 5 log files as backups
handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5
)
handler.setLevel(logging.DEBUG)

# Create a formatter and add it to the handler
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)
