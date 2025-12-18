from datetime import datetime, timezone

from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import TraitSRN
from osa.domain.validation.model import Trait, TraitStatus, Validator
from osa.domain.validation.port.repository import TraitRepository


class RegisterTrait(Command):
    """Register a new trait on this node."""

    srn: TraitSRN
    slug: str
    name: str
    description: str
    validator: Validator


class TraitRegistered(Result):
    """Result of registering a trait."""

    srn: TraitSRN


class RegisterTraitHandler(CommandHandler[RegisterTrait, TraitRegistered]):
    """Handles trait registration."""

    trait_repo: TraitRepository

    async def run(self, cmd: RegisterTrait) -> TraitRegistered:
        trait = Trait(
            srn=cmd.srn,
            slug=cmd.slug,
            name=cmd.name,
            description=cmd.description,
            validator=cmd.validator,
            status=TraitStatus.DRAFT,
            created_at=datetime.now(timezone.utc),
        )
        await self.trait_repo.save(trait)
        return TraitRegistered(srn=cmd.srn)
