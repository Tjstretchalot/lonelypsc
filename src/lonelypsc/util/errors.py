import sys
from typing import List, Optional, Union, cast


def set_context(exc: BaseException, context: Optional[BaseException]) -> None:
    """Sets the context of the exception, merging with the existing one if necessary"""
    if exc.__context__ is None:
        exc.__context__ = context
        return

    if context is None:
        return

    exc.__context__ = combine_multiple_base_exceptions(
        "context", [exc.__context__, context], context=None
    )


if sys.version_info < (3, 11):

    def combine_multiple_base_exceptions(
        msg: str,
        excs: List[BaseException],
        /,
        *,
        context: Optional[BaseException] = None,
    ) -> BaseException:
        """Raises a single BaseException whose __cause__ includes all
        the indicate exceptions and their causes.
        """
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            set_context(excs[0], context)
            return excs[0]

        exc = BaseException(msg)
        last_exc = exc

        for nexc in excs:
            while last_exc.__cause__ is not None:
                last_exc = last_exc.__cause__
            last_exc.__cause__ = nexc

        exc.__context__ = context
        return exc

    def combine_multiple_normal_exceptions(
        msg: str, excs: List[Exception], /, *, context: Optional[BaseException] = None
    ) -> Exception:
        """Raises a single Exception whose __cause__ includes all
        the indicate exceptions and their causes.
        """
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            set_context(excs[0], context)
            return excs[0]

        exc = Exception(msg)
        last_exc: Union[Exception, BaseException] = exc

        for nexc in excs:
            while last_exc.__cause__ is not None:
                last_exc = last_exc.__cause__
            last_exc.__cause__ = nexc

        exc.__context__ = context
        return exc

else:

    def combine_multiple_base_exceptions(
        msg: str,
        excs: List[BaseException],
        /,
        *,
        context: Optional[BaseException] = None,
    ) -> BaseException:
        """Light wrapper around BaseExceptionGroup"""
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            set_context(excs[0], context)
            return excs[0]

        if any(isinstance(e, BaseExceptionGroup) for e in excs):
            new_excs: List[BaseException] = []
            for e in excs:
                if isinstance(e, BaseExceptionGroup):
                    new_excs.extend(e.exceptions)
                else:
                    new_excs.append(e)
            excs = new_excs

        result = BaseExceptionGroup(msg, excs)
        result.__context__ = context
        return result

    def combine_multiple_normal_exceptions(
        msg: str, excs: List[Exception], /, *, context: Optional[BaseException] = None
    ) -> Exception:
        """Light wrapper around ExceptionGroup"""
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            set_context(excs[0], context)
            return excs[0]

        if any(isinstance(e, ExceptionGroup) for e in excs):
            new_excs: List[Exception] = []
            for e in excs:
                if isinstance(e, ExceptionGroup):
                    new_excs.extend(e.exceptions)
                else:
                    new_excs.append(e)
            excs = new_excs

        result = ExceptionGroup(msg, excs)
        result.__context__ = context
        return result


def combine_multiple_exceptions(
    msg: str, excs: List[BaseException], /, *, context: Optional[BaseException] = None
) -> BaseException:
    """Returns a single Exception, if possible, otherwise a BaseException which will
    report all the indicated exceptions in the most informative way possible.
    """
    if all(isinstance(e, Exception) for e in excs):
        return combine_multiple_normal_exceptions(
            msg, cast(List[Exception], excs), context=context
        )
    return combine_multiple_base_exceptions(msg, excs, context=context)
