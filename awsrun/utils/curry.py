from __future__ import annotations
from typing import Callable, Generic, TypeVar, Union

ReturnType = TypeVar("ReturnType")


def curry(num_args: int) -> Callable[[Callable[..., ReturnType]], Partial[ReturnType]]:
    def decorator(fn: Callable[..., ReturnType]):
        return Partial(num_args, fn)

    return decorator


class Partial(Generic[ReturnType]):
    def __init__(
            self, num_args: int, fn: Callable[..., ReturnType], *args, **kwargs
    ) -> None:
        self.num_args = num_args
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *more_args, **more_kwargs) -> Union[Partial[ReturnType], ReturnType]:
        all_args = self.args + more_args  # tuple addition
        all_kwargs = dict(**self.kwargs, **more_kwargs)  # non-mutative dictionary union
        num_args = len(all_args) + len(all_kwargs)
        if num_args >= self.num_args:
            return self.fn(*all_args, **all_kwargs)
        else:
            return Partial(self.num_args, self.fn, *all_args, **all_kwargs)

    def __repr__(self):
        return f"Partial({self.fn}, args={self.args}, kwargs={self.kwargs})"


def curry_functional(num_args: int):
    def decorator(fn: Callable[..., ReturnType]):
        def init(*args, **kwargs):
            def call(*more_args, **more_kwargs):
                all_args = args + more_args
                all_kwargs = dict(**kwargs, **more_kwargs)
                if len(all_args) + len(all_kwargs) >= num_args:
                    return fn(*all_args, **all_kwargs)
                else:
                    return init(*all_args, **all_kwargs)

            return call

        return init()

    return decorator
