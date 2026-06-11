"""Persistence adapters package.

Intentionally does not re-export ``PersistenceProvider`` at the package root:
importing a leaf module (e.g. ``persistence.feature_table``) must not pull in
the whole DI graph, which imports adapters that in turn import persistence leaf
modules — a circular import. Import the provider from its module directly:
``from osa.infrastructure.persistence.di import PersistenceProvider``.
"""
