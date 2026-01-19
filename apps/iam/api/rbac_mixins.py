from __future__ import annotations


class RBACActionMixin:
    """
    Маппинг perms по action.
    Работает с DRF ViewSet (list/retrieve/create/update/partial_update/destroy).
    """
    action_perms: dict[str, tuple[str, ...]] = {}

    require_all: bool = True

    def get_required_perms(self) -> tuple[str, ...]:
        return self.action_perms.get(getattr(self, "action", ""), ())

    @property
    def required_perms(self) -> tuple[str, ...]:
        return self.get_required_perms()
