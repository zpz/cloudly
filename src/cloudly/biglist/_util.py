from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Iterator
from typing import TypeVar

from cloudly.upathlib import Upath
from cloudly.util.seq import Element, Seq

logger = logging.getLogger(__name__)


class FileReader(Seq[Element]):
    """
    A ``FileReader`` is a "lazy" loader of a data file.
    It keeps track of the path of a data file along with a loader function,
    but performs the loading only when needed.
    In particular, upon initiation of a ``FileReader`` object,
    file loading has not happened, and the object
    is light weight and friendly to pickling.

    Once data have been loaded, this class provides various ways to navigate
    the data. At a minimum, the :class:`Seq` API is implemented.

    With loaded data and associated facilities, this object may no longer
    be pickle-able, depending on the specifics of a subclass.

    One use case of this class is to pass around ``FileReader`` objects
    (that are initiated but not loaded) in
    `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ code for concurrent data processing.

    This class is generic with a parameter indicating the type of the elements in the data sequence
    contained in the file. For example you can write::

        def func(file_reader: FileReader[int]):
            ...
    """

    def __repr__(self):
        return f"<{self.__class__.__name__} for '{self.path}'>"

    def __str__(self):
        return self.__repr__()

    @abstractmethod
    def load(self) -> None:
        """
        This method *eagerly* loads all the data from the file into memory.

        Once this method has been called, subsequent data consumption should
        all draw upon this in-memory copy. However, if the data file is large,
        and especially if only part of the data is of interest, calling this method
        may not be the best approach. This all depends on the specifics of the subclass.

        A subclass may allow consuming the data and load parts of data
        in a "as-needed" or "streaming" fashion. In that approach, :meth:`__getitem__`
        and :meth:`__iter__` do not require this method to be called (although
        they may take advantage of the in-memory data if this method *has been called*.).
        """
        raise NotImplementedError


FileReaderType = TypeVar('FileReaderType', bound=FileReader)
"""This type variable indicates the class :class:`FileReader` or a subclass thereof."""


class FileSeq(Seq[FileReaderType]):
    """
    A ``FileSeq`` is a :class:`Seq` of :class:`FileReader` objects.

    Since this class represents a sequence of data files,
    methods such as :meth:`__len__` and :meth:`__iter__` are in terms of data *files*
    rather than data *elements* in the files.
    (One data file contains a sequence of data elements.)
    """

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {self.num_data_items} elements in {self.num_data_files} data file(s)>"

    def __str__(self):
        return self.__repr__()

    @property
    @abstractmethod
    def data_files_info(self) -> list[tuple[str, int, int]]:
        """
        Return a list of tuples for the data files.
        Each tuple, representing one data file, consists of
        "file path", "element count in the file",
        and "cumulative element count in the data files so far".

        Implementation in a subclass should consider caching the value
        so that repeated calls are cheap.
        """
        raise NotImplementedError

    @property
    def num_data_files(self) -> int:
        """Number of data files."""
        return len(self.data_files_info)

    @property
    def num_data_items(self) -> int:
        """Total number of data items in the data files."""
        z = self.data_files_info
        if not z:
            return 0
        return z[-1][-1]

    def __len__(self) -> int:
        """Number of data files."""
        return self.num_data_files

    @abstractmethod
    def __getitem__(self, idx: int) -> FileReaderType:
        """
        Return the :class:`FileReader` for the data file at the specified
        (0-based) index. The returned FileReader object has not loaded data yet,
        and is guaranteed to be pickle-able.

        Parameters
        ----------
        idx
            Index of the file (0-based) in the list of data files as returned
            by :meth:`data_files_info`.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[FileReaderType]:
        """
        Yield one data file at a time.

        .. seealso:: :meth:`__getitem__`
        """
        for i in range(self.__len__()):
            yield self.__getitem__(i)

    @property
    @abstractmethod
    def path(self) -> Upath:
        """
        Return the location (a "directory") where this object
        saves info about the data files, and any other info the implementation chooses
        to save.

        Note that this location does not need to be related to the location of the data files.
        """
        raise NotImplementedError
