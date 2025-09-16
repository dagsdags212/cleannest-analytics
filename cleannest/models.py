import datetime
from pydantic import BaseModel, Field


class Item(BaseModel):
    name: str
    category: str | None
    cost: float
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    deleted_at: datetime.datetime | None = None


class Customer(BaseModel):
    id: str = Field(alias="customer_id")
    name: str = Field(alias="customer_name")
    email: str | None
    phone: str | None
    address: str | None


class Order(BaseModel):
    customer: Customer
    items: list[Item]
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    deleted_at: datetime.datetime | None = None

    def to_list(self) -> list[str]:
        """List order items as (quantity x item_name) elements"""
        return [f"{quantity} x {item_name}" for item_name, quantity in self.quantities().items()]

    def to_dict(self) -> dict[str, any]:
        """Represent order instance as dictionary"""
        return dict(
            customer=self.customer.name,
            created_at=self.created_at,
            updated_at=self.updated_at,
            items=self.to_list(),
            total=self.total(),
        )

    def quantities(self) -> dict[str, int]:
        """Returns a mapping of item names and quantity"""
        item_dict = {}
        for item in self.items:
            if item.name in item_dict:
                item_dict[item.name] += 1
            else:
                item_dict[item.name] = 1
        
        return item_dict

    def total(self) -> float:
        """Compute the total cost of the order"""
        total = 0
        for item in self.items:
            total += item.cost

        return total