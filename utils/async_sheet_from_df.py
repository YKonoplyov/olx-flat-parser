import logging
from itertools import zip_longest
from numbers import Real
import pandas as pd
from pandas import DataFrame
from gspread import Cell
from six import string_types, ensure_text
from gspread_asyncio import AsyncioGspreadWorksheet

logger = logging.getLogger(__name__)

def _escaped_string(value, string_escaping):
    if value in (None, ""):
        return ""
    if string_escaping == "default":
        if value.startswith("'"):
            return "'%s" % value
    elif string_escaping == "off":
        return value
    elif string_escaping == "full":
        return "'%s" % value
    elif callable(string_escaping):
        if string_escaping(value):
            return "'%s" % value
    else:
        raise ValueError(
            "string_escaping parameter must be one of: "
            "'default', 'off', 'full', any callable taking one parameter"
        )
    return value

def _cellrepr(value, allow_formulas, string_escaping):
    """
    Get a string representation of dataframe value.

    :param :value: the value to represent
    :param :allow_formulas: if True, allow values starting with '='
            to be interpreted as formulas; otherwise, escape
            them with an apostrophe to avoid formula interpretation.
    """
    if pd.isnull(value) is True:
        return ""
    if isinstance(value, Real):
        return value
    if not isinstance(value, string_types):
        value = str(value)

    value = ensure_text(value, encoding='utf-8')

    if (not allow_formulas) and value.startswith("="):
        return "'%s" % value
    else:
        return _escaped_string(value, string_escaping)
    

def _index_names(index):
    names = []
    if hasattr(index, "names"):
        names = [ i if i != None else "" for i in index.names ]
    elif index.name not in (None, ""):
        names = [index.name]
    if not any([n not in (None, "") for n in names]):
        names = []
    return names

def _determine_level_count(index):
    if hasattr(index, "levshape"):
        return len(index.levshape)
    return 1

async def async_sheet_from_df(
    worksheet: AsyncioGspreadWorksheet,
    dataframe: DataFrame,
    row: int = 1,
    col: int = 1,
    include_index: bool = False,
    include_column_header: bool = True,
    resize: bool = False,
    allow_formulas=True,
    string_escaping="default",
):
    y, x = dataframe.shape
    index_col_size = 0
    column_header_size = 0
    index_names = _index_names(dataframe.index)
    column_names_not_labels = _index_names(dataframe.columns)
    if include_index:
        index_col_size = _determine_level_count(dataframe.index)
        x += index_col_size
    if include_column_header:
        column_header_size = _determine_level_count(dataframe.columns)
        y += column_header_size
        # if included index has name(s) it needs its own header row to accommodate columns' index names
        if column_header_size > 1 and include_index and index_names:
            y += 1
    if row > 1:
        y += row - 1
    if col > 1:
        x += col - 1
    if resize:
        worksheet.resize(y, x)


    updates = []

    if include_column_header:
        elts = list(dataframe.columns)
        # if columns object is multi-index, it will span multiple rows
        extra_header_row = None
        if column_header_size > 1:
            elts = list(dataframe.columns)
            if include_index:
                extra = tuple(column_names_not_labels) \
                        if column_names_not_labels \
                        else ("",) * column_header_size
                extra = [ extra ]
                if index_col_size > 1:
                    extra = extra + [ ("",) * column_header_size ] * (index_col_size - 1)
                elts = extra + elts
                # if index has names, they need their own header row
                if index_names:
                    extra_header_row = list(index_names) + [ "" ] * len(dataframe.columns)
            for level in range(0, column_header_size):
                for idx, tup in enumerate(elts):
                    updates.append(
                        (
                            row,
                            col + idx,
                            _cellrepr(
                                tup[level], allow_formulas, string_escaping
                            ),
                        )
                    )
                row += 1
            if extra_header_row:
                for idx, val in enumerate(extra_header_row):
                    updates.append(
                        (
                            row,
                            col + idx,
                            _cellrepr(
                                val, allow_formulas, string_escaping
                            ),
                        )
                    )
                row += 1

        else:
            # columns object is not multi-index, columns object's "names"
            # can not be written anywhere in header and be parseable to pandas.
            elts = list(dataframe.columns)
            if include_index:
                # if index has names, they do NOT need their own header row
                if index_names:
                    elts = index_names + elts
                else:
                    elts = ([""] * index_col_size) + elts
            for idx, val in enumerate(elts):
                updates.append(
                    (
                        row,
                        col + idx,
                        _cellrepr(val, allow_formulas, string_escaping),
                    )
                )
            row += 1

    values = []
    for value_row, index_value in zip_longest(
        dataframe.to_numpy('object'), dataframe.index.to_numpy('object')
    ):
        if include_index:
            if not isinstance(index_value, (list, tuple)):
                index_value = [index_value]
            value_row = list(index_value) + list(value_row)
        values.append(value_row)
    for y_idx, value_row in enumerate(values):
        for x_idx, cell_value in enumerate(value_row):
            updates.append(
                (
                    y_idx + row,
                    x_idx + col,
                    _cellrepr(cell_value, allow_formulas, string_escaping),
                )
            )

    if not updates:
        logger.debug("No updates to perform on worksheet.")
        return

    cells_to_update = [Cell(row, col, value) for row, col, value in updates]
    logger.debug("%d cell updates to send", len(cells_to_update))

    resp = await worksheet.update_cells(
        cells_to_update, value_input_option="USER_ENTERED"
    )
    logger.debug("Cell update response: %s", resp)

