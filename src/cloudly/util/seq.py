from __future__ import annotations

__all__ = ['Seq', 'Slicer', 'Chain']


import bisect
import itertools
from collections.abc import Iterator, Sequence
from typing import Protocol, TypeVar, runtime_checkable


def locate_idx_in_chunked_seq(
    idx: int,
    len_cumsum: Sequence[int],
    last_chunk: None | tuple[int, int, int] = None,
):
    """
    Suppose a sequence is composed of a number of member sequences.
    This function finds which member sequence contains the requested item.

    Parameters
    ----------
    idx
        Index of the item of interest.
    len_cumsum
        Cumulative lengths of the member sequences.
    last_chunk
        Info about the last call to this function, consisting of

        ::

            ('index of the selected member sequence',
             'starting index of the chosen sequence',
             'finishing index (plus 1) of the chosen sequence',
            )

        This info about the last call is used as the starting point to
        search for the current item of interest, at index ``idx``.
        This is used with the assumption that user tends to access consecutive
        items.
    """
    if idx < 0:
        idx = len_cumsum[-1] + idx
    if idx < 0 or idx > len_cumsum[-1]:
        raise IndexError(idx)

    igrp0 = None
    igrp = None
    last_group = last_chunk
    if last_group is not None:
        igrp0 = last_group[0]
        if idx < last_group[1]:
            igrp = bisect.bisect_right(len_cumsum, idx, hi=igrp0)
        elif idx < last_group[2]:
            igrp = igrp0
        else:
            igrp = bisect.bisect_right(len_cumsum, idx, lo=igrp0 + 1)
    else:
        igrp = bisect.bisect_right(len_cumsum, idx)
    if igrp != igrp0:
        if igrp == 0:
            last_chunk = (
                0,  # group index
                0,  # item index lower bound
                len_cumsum[0],  # item index upper bound
            )
        else:
            last_chunk = (
                igrp,
                len_cumsum[igrp - 1],
                len_cumsum[igrp],
            )

    # index of chunk, item index in chunk, book-keeping for the chosen chunk
    # `last_chunk` is the value to be passed in to this function in the next call.
    return igrp, idx - last_chunk[1], last_chunk


Element = TypeVar('Element')
"""
This type variable is used to annotate the type of a data element.
"""


@runtime_checkable
class Seq(Protocol[Element]):
    """
    The protocol ``Seq`` is simpler and broader than the standard |Sequence|_.
    The former requires/provides only ``__len__``, ``__getitem__``, and ``__iter__``,
    whereas the latter adds ``__contains__``, ``__reversed__``, ``index`` and ``count``
    to these three. Although the extra methods can be implemented using the three basic methods,
    they could be massively inefficient in particular cases, and that is the case
    in the applications targeted by ``cloudly.biglist``.
    For this reason, the classes defined in this package implement the protocol ``Seq``
    rather than ``Sequence``, to prevent the illusion that methods ``__contains__``, etc.,
    are usable.

    A class that implements this protocol is sized, iterable, and subscriptable by an int index.
    This is a subset of the methods provided by ``Sequence``.
    In particular, ``Sequence`` implements this protocol, hence is considered a subclass
    of ``Seq`` for type checking purposes:

    >>> from cloudly.util.seq import Seq
    >>> from collections.abc import Sequence
    >>> issubclass(Sequence, Seq)
    True

    The built-in dict and tuple also implement the ``Seq`` protocol.

    The type parameter ``Element`` indicates the type of each data element.
    """

    # The subclass check is not exactly right.
    # This protocol requires the method ``__getitem__``
    # to take an int key and return an element, but
    # the subclass check does not enforce this signature.
    # For example, dict would pass this check but it is not
    # an intended subclass.

    # An alternative definition is a `Subscriptable` following examples
    # in "cpython/Lib/_collections_abc.py", then a `Seq` inheriting from
    # Sized, Iterable, and Subscriptable.

    def __len__(self) -> int: ...

    def __getitem__(self, index: int) -> Element: ...

    def __iter__(self) -> Iterator[Element]:
        # A reference, or naive, implementation.
        for i in range(self.__len__()):
            yield self[i]


class Slicer(Seq[Element]):
    """
    The class :class:`Slicer` takes any :class:`Seq` and provides :meth:`~Slicer.__getitem__` that accepts
    a single index, or a slice, or a list of indices. A single-index access will return
    the requested element; the other two scenarios return a new Slicer via a zero-copy operation.
    To get all the elements out of a Slicer, either iterate it or call its method :meth:`~Slicer.collect`.

    A ``Slicer`` object makes "zero-copy"---it holds a reference to the underlying ``Seq``
    and keeps track of indices of the selected elements.
    A ``Slicer`` object may be sliced again in a repeated "zoom in" fashion.
    Actual data elements are retrieved from the underlying ``Seq``
    only when a single-element is accessed or iteration is performed.
    In other words, until an actual data element needs to be returned, it's all operations on the indices.
    """

    def __init__(self, list_: Seq[Element], range_: None | range | Seq[int] = None):
        """
        This provides a "slice" of, or "window" into, ``list_``.

        The selection of elements is represented by the optional ``range_``,
        which is eithe a `range <https://docs.python.org/3/library/stdtypes.html#range>`_
        such as ``range(3, 8)``,
        or a list of indices such as ``[1, 3, 5, 6]``.
        If ``range_`` is ``None``, the "window" covers the entire ``list_``.
        A common practice is to create a ``Slicer`` object without ``range_``,
        and then access a slice of it,
        for example, ``Slicer(obj)[3:8]`` rather than ``Slicer(obj, range(3,8))``.

        During the use of this object, the underlying ``list_`` must remain unchanged.
        Otherwise purplexing and surprising things may happen.
        """
        self._list = list_
        self._range = range_

    def __repr__(self):
        return f'<{self.__class__.__name__} into {self.__len__()}/{len(self._list)} of {self._list!r}>'

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        """Number of elements in the current window or "slice"."""
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __getitem__(self, idx: int | slice | Seq[int]):
        """
        Element access by a single index, slice, or an index array.
        Negative index and standard slice syntax work as expected.

        Single-index access returns the requested data element.
        Slice and index-array accesses return a new :class:`Slicer` object,
        which, naturally, can be sliced again, like

        ::

            >>> x = list(range(30))
            >>> Slicer(x)[[1, 3, 5, 6, 7, 8, 9, 13, 14]][::2][-2]
            9
        """
        if isinstance(idx, int):
            # Return a single element.
            if self._range is None:
                return self._list[idx]
            return self._list[self._range[idx]]

        # Return a new `Slicer` object below.

        if isinstance(idx, slice):
            if self._range is None:
                range_ = range(len(self._list))[idx]
            else:
                range_ = self._range[idx]
            return self.__class__(self._list, range_)

        # `idx` is a list of indices.
        if self._range is None:
            return self.__class__(self._list, idx)
        return self.__class__(self._list, [self._range[i] for i in idx])

    def __iter__(self) -> Iterator[Element]:
        """Iterate over the elements in the current window or "slice"."""
        if self._range is None:
            yield from self._list
        else:
            # This could be inefficient, depending on
            # the random-access performance of `self._list`.
            for i in self._range:
                yield self._list[i]

    @property
    def raw(self) -> Seq[Element]:
        """
        Return the underlying data :class:`Seq`, that is, the ``list_``
        that was passed into :meth:`__init__`.
        """
        return self._list

    @property
    def range(self) -> None | range | Seq[int]:
        """
        Return the parameter ``range_`` that was provided to :meth:`__init__`,
        representing the selection of items in the underlying ``Seq``.
        """
        return self._range

    def collect(self) -> list[Element]:
        """
        Return a list containing the elements in the current window.
        This is equivalent to ``list(self)``.

        This is often used to substantiate a small slice as a list, because a slice is still a :class:`Slicer` object,
        which does not directly reveal the data items. For example,

        ::

            >>> x = list(range(30))
            >>> Slicer(x)[3:11]
            <Slicer into 8/30 of [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]>
            >>> Slicer(x)[3:11].collect()
            [3, 4, 5, 6, 7, 8, 9, 10]

        (A list is used for illustration. In reality, list supports slicing directly, hence would not need ``Slicer``.)

        .. warning:: Do not call this on "big" data!
        """
        return list(self)


class Chain(Seq[Element]):
    """
    This class tracks a series of :class:`Seq` objects to provide
    random element access and iteration on the series as a whole,
    with zero-copy.

    Example ::

        >>> from cloudly.util.seq import Chain
        >>> numbers = list(range(10))
        >>> car_data  # doctest: +SKIP
        <ParquetBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
        >>> combined = Chain(numbers, car_data)
        >>> combined[3]
        3
        >>> combined[9]
        9
        >>> combined[10]
        {'make': 'ford', 'year': 1960, 'sales': 234}
        >>>
        >>> car_data[0]
        {'make': 'ford', 'year': 1960, 'sales': 234}

    This class is in contrast with the standard `itertools.chain <https://docs.python.org/3/library/itertools.html#itertools.chain>`_,
    which takes iterables.
    """

    def __init__(self, list_: Seq[Element], *lists: Seq[Element]):
        self._lists = (list_, *lists)
        self._lists_len: None | list[int] = None
        self._lists_len_cumsum: None | list[int] = None
        self._len: None | int = None

        # Records info about the last call to `__getitem__`
        # to hopefully speed up the next call, under the assumption
        # that user tends to access consecutive or neighboring
        # elements.
        self._get_item_last_list = None

    def __repr__(self):
        return "<{} with {} elements in {} member Seq's>".format(
            self.__class__.__name__,
            self.__len__(),
            len(self._lists),
        )

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        if self._len is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._len = sum(self._lists_len)
        return self._len

    def __getitem__(self, idx: int) -> Element:
        if self._lists_len_cumsum is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._lists_len_cumsum = list(itertools.accumulate(self._lists_len))
        ilist, idx_in_list, list_info = locate_idx_in_chunked_seq(
            idx, self._lists_len_cumsum, self._get_item_last_list
        )
        self._get_item_last_list = list_info
        return self._lists[ilist][idx_in_list]

    def __iter__(self) -> Iterator[Element]:
        for v in self._lists:
            yield from v

    @property
    def raw(self) -> tuple[Seq[Element], ...]:
        """
        Return the underlying list of :class:`Seq`\\s.

        A member ``Seq`` could be a :class:`Slicer`. The current method
        does not follow a ``Slicer`` to its "raw" component, b/c
        that could represent a different set of elements than the ``Slicer``
        object.
        """
        return self._lists
