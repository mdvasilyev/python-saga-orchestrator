from enum import StrEnum


class BaseStrEnum(StrEnum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Return the upper-cased version of the member name.
        """
        return name.upper()