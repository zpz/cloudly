"""
The module `biglist` provides persisted, out-of-memory Python data structures
that implement the :class:`Seq` interface (a simplified |Sequence|_) with the capabilities of
concurrent and distributed reading and writing.
The main use case is *sequentially* processing large amounts of data that can not fit in memory.

Currently, two kinds of "biglists" are provided, namely ``Biglist`` and ``ExternalBiglist``.

:class:`Biglist` manages writing and reading.
Here, "writing" refers to adding data to this facility and be managed by it.
The class manages its data files in addition to meta info.
Writing is append-only; updating existing data is not supported.
Appending can be conducted by a number of distributed workers.

:class:`ExternalBiglist` defines a kind of "external biglist". When given the paths to a set of
pre-existing data files in the Apache Parquet format,
this class provides a rich set of facilities for *reading* the data.
(Potentially confusingly, ``Biglist`` can save data in Parquet format among others; that type of a ``Biglist``
is *not* a ``ExternalBiglist``, which supports *reading* only.)

``Biglist`` and ``ExternalBiglist`` share the same core API for *reading*.
Although random element access is supported, it is not optimized
and is not the target usage pattern. Random element access can be *very* inefficient.
The intended way of data consumption is by iteration, which
can be done *collectively* by distributed workers.

Persistence can be on local disk or in cloud storage.
Thanks to the module `cloudly.upathlib`,
the implementation as wel as the end-user API is agnostic to the location of storage.

In support of ``ExternalBiglist``, some utilities are provided for reading and writing Parquet data files.
These utilities are useful in their own right.

Additional utilities provide mechanisms for "slicing and dicing" a biglist,
as well as "chaining up" a series of biglists. These utilities work not only for biglist,
but also for any :class:`Seq`.
"""

__all__ = [
    'Biglist',
    'BiglistFileReader',
    'ExternalBiglist',
    'ParquetFileReader',
    'FileReader',
    'parquet',
    'Chain',
    'Slicer',
]


from cloudly.util.seq import Chain, Slicer

from . import parquet
from ._biglist import (
    Biglist,
)
from ._external_biglist import (
    ExternalBiglist,
    ParquetFileReader,
)
from ._util import BiglistFileReader, FileReader
