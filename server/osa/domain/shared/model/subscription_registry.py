"""Subscription registry mapping event types to consumer groups.

Built from the HANDLERS list at startup, this registry tells the Outbox
how many delivery rows to create for each event type.
"""

from typing import NewType

SubscriptionRegistry = NewType("SubscriptionRegistry", dict[str, set[str]])
"""Mapping of event_type_name → set of consumer_group_names.

Built at startup from the HANDLERS list by mapping each handler's
``__event_type__.__name__`` to the handler's ``__name__``.
"""
