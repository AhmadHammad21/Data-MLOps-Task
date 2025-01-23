import logging
from logging.handlers import RotatingFileHandler


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RecommendationService")
handler = RotatingFileHandler("logging/service.log", maxBytes=1000000, backupCount=3)
logger.addHandler(handler)