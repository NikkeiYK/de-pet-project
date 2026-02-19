import requests
from utils.logger import get_logger
from models.shop_model import FakeShopModel
logger = get_logger(__name__)


def fetch_fake_shop_data():
    logger.info(" Запрос товаров из магазина")
    base_url = 'https://fakestoreapi.com/products'

    try:
        response = requests.get(base_url)
        response.raise_for_status()
        data_json = response.json()
        validated_data = FakeShopModel(data_json)
        return validated_data
    except Exception as e:
        logger.error(f"Ошибка при получении товаров: {e}")
        return []
