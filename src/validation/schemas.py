"""Pydantic schemas for bronze validation."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, EmailStr, Field


class CustomerRecord(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: EmailStr
    signup_date: date
    loyalty_tier: Literal["Bronze", "Silver", "Gold", "Platinum"]
    signup_channel: str


class OrderRecord(BaseModel):
    order_id: str
    customer_id: str
    order_date: date
    gross_total: float = Field(ge=0)
    net_total: float = Field(ge=0)
    payment_method: str


class ProductRecord(BaseModel):
    product_id: int
    product_name: str
    category: str
    unit_price: float = Field(ge=0)
    cost_price: float = Field(ge=0)
    inventory_quantity: int = Field(ge=0)
