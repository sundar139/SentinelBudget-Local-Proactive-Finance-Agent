"""Typed explicit-SQL repositories for SentinelBudget."""

from sentinelbudget.db.repositories.accounts import Account, AccountRepository
from sentinelbudget.db.repositories.budgets import Budget, BudgetRepository
from sentinelbudget.db.repositories.categories import Category, CategoryRepository
from sentinelbudget.db.repositories.goals import Goal, GoalRepository
from sentinelbudget.db.repositories.insights import Insight, InsightRepository
from sentinelbudget.db.repositories.ledger import LedgerEntry, LedgerRepository
from sentinelbudget.db.repositories.preferences import UserPreference, UserPreferenceRepository
from sentinelbudget.db.repositories.session import get_db_connection, transaction
from sentinelbudget.db.repositories.users import User, UserRepository

__all__ = [
    "Account",
    "AccountRepository",
    "Budget",
    "BudgetRepository",
    "Category",
    "CategoryRepository",
    "Goal",
    "GoalRepository",
    "Insight",
    "InsightRepository",
    "LedgerEntry",
    "LedgerRepository",
    "User",
    "UserPreference",
    "UserPreferenceRepository",
    "UserRepository",
    "get_db_connection",
    "transaction",
]
