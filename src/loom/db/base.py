"""Shared SQLAlchemy DeclarativeBase for all Loom tables."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Loom's SQLAlchemy declarative base. All ORM tables inherit from this."""
