"""
Base class for pharmacy location eligibility rules.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from utils.database import Database


class BaseRule(ABC):
    """
    Abstract base class for all pharmacy location rules.
    """

    def __init__(self, db: Database):
        self.db = db

    @property
    @abstractmethod
    def rule_name(self) -> str:
        """Human-readable name of the rule."""
        pass

    @property
    @abstractmethod
    def item_number(self) -> str:
        """Item number from the handbook (e.g., 'Item 130')."""
        pass

    @abstractmethod
    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if a property is eligible under this rule.

        Args:
            property_data: Dictionary containing property information including:
                - address: str
                - latitude: float
                - longitude: float
                - (other fields as needed)

        Returns:
            Tuple of (is_eligible, evidence):
                - is_eligible: True if property qualifies under this rule
                - evidence: String describing why property qualifies, or None
        """
        pass

    def format_evidence(self, **kwargs) -> str:
        """
        Format evidence string for this rule.

        Args:
            **kwargs: Key-value pairs to include in evidence

        Returns:
            Formatted evidence string
        """
        parts = []
        for key, value in kwargs.items():
            if value is not None:
                parts.append(f"{key}: {value}")

        return " | ".join(parts)

    def __str__(self) -> str:
        return f"{self.item_number}: {self.rule_name}"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.item_number}>"
