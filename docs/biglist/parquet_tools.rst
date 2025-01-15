.. testsetup:: *

  from cloudly.upathlib import LocalUpath
  from cloudly.biglist.parquet import write_arrays_to_parquet
  from cloudly.biglist import ExternalBiglist
  from cloudly.util.seq import Slicer

  path = LocalUpath('/tmp/a/b/c/e')
  path.rmrf()

  year = list(range(1970, 2021))
  make = ['honda'] * len(year)
  sales = list(range(123, 123 + len(make)))
  write_arrays_to_parquet([make, year, sales], path / 'honda.parquet', names=['make', 'year', 'sales'], row_group_size=10)

  year = list(range(1960, 2021))
  make = ['ford'] * len(year)
  sales = list(range(234, 234 + len(make)))
  write_arrays_to_parquet([make, year, sales], path / 'ford.parquet', names=['make', 'year', 'sales'], row_group_size=10)

  car_data = ExternalBiglist.new(path, storage_format='parquet')

.. testcleanup::

   LocalUpath('/tmp/a/b/c').rmrf()



***************************
Utilities for Parquet files
***************************


Reading Parquet files
---------------------

The function :func:`read_parquet_file` is provided to read a single Parquet file independent of
:class:`ExternalBiglist`. It returns a :class:`ParquetFileReader`. All the facilities of this class,
as demonstrated above, are ready for use:

>>> [v.path for v in car_data.files]
[LocalUpath('/tmp/a/b/c/e/ford.parquet'), LocalUpath('/tmp/a/b/c/e/honda.parquet')]
>>>
>>> from cloudly.biglist.parquet import read_parquet_file
>>> ff = read_parquet_file(car_data.files[1].path)
>>> ff
<ParquetFileReader for '/tmp/a/b/c/e/honda.parquet'>
>>> len(ff)
51
>>> ff.column_names
['make', 'year', 'sales']
>>> ff[3]
{'make': 'honda', 'year': 1973, 'sales': 126}
>>> Slicer(ff.columns(['year', 'sales']))[10:16].collect()
[{'year': 1980, 'sales': 133}, {'year': 1981, 'sales': 134}, {'year': 1982, 'sales': 135}, {'year': 1983, 'sales': 136}, {'year': 1984, 'sales': 137}, {'year': 1985, 'sales': 138}]
>>> ff.num_row_groups
6
>>> ff.row_group(3).column('sales')  # doctest: +ELLIPSIS
<pyarrow.lib.ChunkedArray object at 0x7...>
[
  [
    153,
    154,
    155,
    156,
    157,
    158,
    159,
    160,
    161,
    162
  ]
]

Writing Parquet files
---------------------

The function :func:`write_arrays_to_parquet` is provided to write data columns to a single Parquet file.

>>> from uuid import uuid4
>>> from cloudly.biglist.parquet import write_arrays_to_parquet, read_parquet_file
>>> import random
>>> from cloudly.upathlib import LocalUpath
>>> N = 10000
>>> path = LocalUpath('/tmp/a/b/c/d')
>>> path.rmrf()
0
>>> write_arrays_to_parquet([[random.randint(0, 10000) for _ in range(N)], [str(uuid4()) for _ in range(N)]], path / 'data.parquet', names=['key', 'value'])
>>> f = read_parquet_file(path / 'data.parquet')
>>> f
<ParquetFileReader for '/tmp/a/b/c/d/data.parquet'>
>>> len(f)
10000
>>> f.metadata   # doctest: +ELLIPSIS
<pyarrow._parquet.FileMetaData object at 0x7...>
  created_by: parquet-cpp-arrow version 1...
  num_columns: 2
  num_rows: 10000
  num_row_groups: 1
  format_version: 2.6
  serialized_size: 6...
>>> f.metadata.schema  # doctest: +ELLIPSIS
<pyarrow._parquet.ParquetSchema object at 0x7...>
required group field_id=-1 schema {
  optional int64 field_id=-1 key;
  optional binary field_id=-1 value (String);
}
<BLANKLINE>
>>>

Similarly, :func:`write_pylist_to_parquet` writes data rows to a Parquet file:

>>> from cloudly.biglist.parquet import write_pylist_to_parquet
>>> data = [{'name': str(uuid4()), 'age': random.randint(1, 100), 'income': {'employer': str(uuid4()), 'amount': random.randint(10000, 100000)}} for _ in range(100)]
>>> f = LocalUpath('/tmp/test/data.parquet')
>>> f.rmrf()  # doctest: +SKIP
0
>>> write_pylist_to_parquet(data, f)
>>> ff = read_parquet_file(f)
>>> ff[0]  # doctest: +SKIP
{'name': '066ced72-fd33-492a-9180-39eeca541b1a', 'age': 75, 'income': {'amount': 17840, 'employer': 'bfc176a0-5257-4913-bd1e-3c4d51885e0c'}}
>>> ff[11]  # doctest: +SKIP
{'name': 'a239af28-41ff-4215-b560-9c45db15478e', 'age': 12, 'income': {'amount': 17488, 'employer': 'e97f70c9-1659-4fa6-9123-eb39779d00d6'}}
>>>
