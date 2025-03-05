from typing import Any, Callable

QueryExecutorFunc = Callable[[Any], bool]
GetVariableFunc = Callable[[Any], Any]


class QueryExecutor:
    _func: QueryExecutorFunc

    def __init__(self, func: QueryExecutorFunc) -> None:
        self._func = func

    def __call__(self, obj: Any) -> bool:
        return self._func(obj)

    def __and__(self, other: QueryExecutorFunc) -> "QueryExecutor":
        return QueryExecutor(lambda obj: self(obj) and other(obj))

    def __or__(self, other: QueryExecutorFunc) -> "QueryExecutor":
        return QueryExecutor(lambda obj: self(obj) or other(obj))


class Query:
    _getvar: GetVariableFunc

    @staticmethod
    def _default_getvar(obj: Any) -> Any:
        return obj

    def __init__(self, getvar: GetVariableFunc | None = None) -> None:
        self._getvar = getvar or self._default_getvar

    def __eq__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, other: Any
    ) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) == other)

    def __ne__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, other: Any
    ) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) != other)

    def contains(self, lhs: Any) -> QueryExecutor:
        return QueryExecutor(lambda obj: lhs in self._getvar(obj))

    def __lt__(self, rhs: Any) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) < rhs)

    def __le__(self, rhs: Any) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) <= rhs)

    def __gt__(self, rhs: Any) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) > rhs)

    def __ge__(self, rhs: Any) -> QueryExecutor:
        return QueryExecutor(lambda obj: self._getvar(obj) >= rhs)

    def _get_attr_or_item(self, obj: Any, name: str) -> Any:
        var = self._getvar(obj)

        try:
            return var[name]
        except TypeError:
            return getattr(var, name)

    def __getattr__(self, name: str) -> "Query":
        return Query(lambda obj: self._get_attr_or_item(obj, name))

    def __getitem__(self, name: str) -> "Query":
        return Query(lambda obj: self._get_attr_or_item(obj, name))
