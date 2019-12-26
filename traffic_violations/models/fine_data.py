from dataclasses import dataclass


@dataclass(frozen=True)
class FineData:
    """ Fine data for a response """

    FINE_FIELDS = ('fined', 'reduced', 'paid', 'outstanding',)

    fined: float = 0.0
    outstanding: float = 0.0
    paid: float = 0.0
    reduced: float = 0.0

    def __iter__(self):
        for field in self.FINE_FIELDS:
            yield (field, getattr(self, field))

    def fines_assessed(self) -> bool:
        return any(getattr(self, field) > 0 for field in self.FINE_FIELDS)

    def max_amount(self) -> float:
        return max(getattr(self, field) for field in self.FINE_FIELDS)

