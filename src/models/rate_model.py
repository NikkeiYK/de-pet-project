from pydantic import BaseModel, RootModel, Field
from typing import Dict

ConversionModel = RootModel[Dict[str, float]]


class RateModel(BaseModel):
    result: str = Field(..., description="Статус запроса (success/error)")
    documentation: str = Field(..., description="Ссылка на документацию")
    terms_of_use: str = Field(...,
                              description="Ссылка на условия использования")

    # Время последнего обновления (Unix timestamp)
    time_last_update_unix: int = Field(...,
                                       description="Unix время последнего обновления")

    # Время последнего обновления (Читаемый формат)
    time_last_update_utc: str = Field(...,
                                      description="UTC время последнего обновления")

    # Время следующего обновления
    time_next_update_unix: int = Field(...,
                                       description="Unix время следующего обновления")
    time_next_update_utc: str = Field(...,
                                      description="UTC время следующего обновления")

    base_code: str = Field(..., description="Базовая валюта (например, USD)")

    # Курсы валют
    conversion_rates: ConversionModel = Field(
        ..., description="Словарь с курсами валют")
