from pydantic import BaseModel, RootModel
from typing import List


class ShopItem(BaseModel):
    id: int
    title: str
    price: float
    description: str
    category: str
    image: str


FakeShopModel = (RootModel[List[ShopItem]])
