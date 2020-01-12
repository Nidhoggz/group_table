# -*- coding:utf-8 -*-


from collections import OrderedDict
from pprint import pprint

_SUM_SUFFIX = "__sum"
_MAX_SUFFIX = "__max"
_MIN_SUFFIX = "__min"
_COUNT_SUFFIX = "__count"
_AVG_SUFFIX = "__avg"


def combination(lists, code="-"):
    try:
        import reduce
    except:
        from functools import reduce

    def func(list1, list2):
        return [str(i) + code + str(j) for i in list1 for j in list2]

    return reduce(func, lists)


class Row(object):
    def __init__(self, **kwargs):
        self._cols = []
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return "Row[{}]".format(",".join(["{}={}".format(col, getattr(self, col, None)) for col in self._cols]))

    @property
    def description(self):
        return self._cols

    def batch_add_cols(self, cols):
        for k, v in cols.iteritems():
            setattr(self, k, v)

    def has_col(self, col_name):
        return True if getattr(self, col_name, None) is not None else False

    def generate_group_key(self, cols):
        return ",".join([col + "=" + str(getattr(self, col, "")) for col in cols])

    def fetchall(self):
        return [getattr(self, col, None) for col in self._cols]

    def __setattr__(self, key, value):
        self.__dict__[key] = value
        if key == "_cols":
            return
        self._cols.append(key)


class Group(object):
    def __init__(self, **kwargs):
        keys = OrderedDict(kwargs if kwargs is not None else {})
        self._key = ",".join(["{}={}".format(k, v) for k, v in keys.iteritems()])
        self._protect_key = keys.keys()
        self._protect = keys
        self._rows = []
        self._select = []
        self._result = OrderedDict({})

    def __repr__(self):
        return "Group[key:{},rows_len:{},rows:{}]".format(self._key, len(self._rows), self._rows)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value
        self._protect_key = [s.split("=")[0] for s in value.split(",")]
        self._protect = OrderedDict({s.split("=")[0]: s.split("=")[1] for s in value.split(",")})

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, value):
        for v in value:
            for k in self._protect_key:
                if not v.has_col(k):
                    raise Exception("Group Row not find protect key")
        self._rows = value

    def add_rows(self, rows):
        self._rows.extend(rows)

    def add_protect_col(self, col_name, col_value=None):
        for row in self._rows:
            if col_value is None:
                col_value = getattr(row, col_name, None)
            if col_value is not None:
                self._protect[col_name] = col_value
                if col_name not in self._protect_key:
                    self._protect_key.append(col_name)
                break

    def select(self, *col_names):
        self._select = list(col_names)

    def get_select_result(self):
        for col_name in self._select:
            if col_name in self._protect:
                self._result[col_name] = self._protect[col_name]
            else:
                for row in self._rows:
                    col_value = getattr(row, col_name, None)
                    if col_value is not None:
                        self._result[col_name] = col_value
                        break
                if self._result.get(col_name) is None:
                    self._result[col_name] = None
        return self._result

    def get_select_row(self):
        for col_name in self._select:
            if col_name in self._protect:
                self._result[col_name] = self._protect[col_name]
            else:
                for row in self._rows:
                    col_value = getattr(row, col_name, None)
                    if col_value is not None:
                        self._result[col_name] = col_value
                        break
                if self._result.get(col_name) is None:
                    self._result[col_name] = None

        return Row(**self._result)

    def sum(self, col_name):
        single_cols = []
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                try:
                    int(col_value)
                except:
                    raise
                else:
                    single_cols.append(int(col_value))
        return sum(single_cols)

    def max(self, col_name):
        if len(self._rows) == 0:
            return None
        max_col = getattr(self._rows[0], col_name)
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                if col_value > max_col:
                    max_col = col_value
        return max_col

    def min(self, col_name):
        if len(self._rows) == 0:
            return None
        min_col = getattr(self._rows[0], col_name)
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                if col_value < min_col:
                    min_col = col_value
        return min_col

    def avg(self, col_name):
        cols = []
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                cols.append(col_value)
        if len(cols) == 0:
            return 0
        return sum(cols) / len(cols)

    def count(self, col_name="*"):
        if col_name == "*":
            return len(self._rows)
        cols = []
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                cols.append(col_value)
        return len(cols)

    def distinct(self, col_name):
        cols = set([])
        for row in self._rows:
            col_value = getattr(row, col_name, None)
            if col_value is not None:
                cols.add(col_value)
        return list(cols)


class Table(object):

    def __init__(self, name):
        self._name = name
        self._groups = []
        self._group_index = {}

    def __repr__(self):
        return "Table[{}]".format(",".join([repr(group) for group in self._groups]))

    @property
    def name(self):
        return self.name

    @name.setter
    def name(self, value):
        self._name = value

    def _build_group_index(self):
        for group in self._groups:
            self._group_index[group.key] = group

    def get_group_by_index(self, key):
        return self._group_index.get(key)

    def load_data(self, descs, raw_rows):
        rows = []
        for row in raw_rows:
            kwargs = {str(col[0]): col[1] for col in zip(descs, row)}
            rows.append(Row(**kwargs))
        g = Group()
        g.add_rows(rows)
        self._groups.append(g)
        self._build_group_index()

    def group_by(self, *col_names):
        if len(col_names) == 0:
            return self
        rows = []
        for group in self._groups:
            rows.extend(group.rows)
        self._groups = []
        g = Group()
        g.add_rows(rows)
        option_lists = []
        for col_name in col_names:
            options = [col_name + "=" + str(option) for option in g.distinct(col_name)]
            option_lists.append(options)
        keys = combination(option_lists, code=",")
        del g
        for key in keys:
            new_g = Group()
            new_g.key = key
            self._groups.append(new_g)
        self._build_group_index()
        for row in rows:
            row_key = row.generate_group_key(col_names)
            self.get_group_by_index(row_key).add_rows([row, ])
        return self

    def sum(self, col_name, alias=None):
        for group in self._groups:
            sum_col = group.sum(col_name)
            if alias is None:
                alias = col_name + _SUM_SUFFIX
            group.add_protect_col(alias, sum_col)
        return self

    def max(self, col_name, alias=None):
        for group in self._groups:
            max_col = group.max(col_name)
            if alias is None:
                alias = col_name + _MAX_SUFFIX
            group.add_protect_col(alias, max_col)
        return self

    def min(self, col_name, alias=None):
        for group in self._groups:
            min_col = group.min(col_name)
            if alias is None:
                alias = col_name + _MIN_SUFFIX
            group.add_protect_col(alias, min_col)
        return self

    def count(self, col_name="*", alias=None):
        for group in self._groups:
            count_col = group.count(col_name)
            if alias is None:
                alias = col_name + _COUNT_SUFFIX
            group.add_protect_col(alias, count_col)
        return self

    def avg(self, col_name, alias=None):
        for group in self._groups:
            avg_col = group.avg(col_name)
            if alias is None:
                alias = col_name + _AVG_SUFFIX
            group.add_protect_col(alias, avg_col)
        return self

    def distinct(self, col_name, alias=None):
        for group in self._groups:
            distinct_col = group.distinct(col_name)
            if alias is None:
                alias = col_name
            group.add_protect_col(alias, distinct_col)
        return self

    def select(self, *col_names):
        for group in self._groups:
            group.select(*col_names)
        return self

    @property
    def desription(self):
        if len(self._groups) == 0:
            return []
        for group in self._groups:
            return group.get_select_row().description

    def fetchall(self):
        raw_rows = []
        for group in self._groups:
            row = group.get_select_row()
            raw_rows.append(row.fetchall())
        return raw_rows


if __name__ == '__main__':
    # r = Row(id=1, name="zyf")
    # r.batch_add_cols({"age": 18, "sex": "man"})
    # print r
    # r2 = Row(id=2, name="zyfa", age=12)
    # r3 = Row(id=3, name="zyf1", age=14)
    # r4 = Row(id=4, name="zyf23", age=17)
    # g = Group(sex="man")
    # g.add_rows([r, r2, r3, r4])
    # print g
    # print g.sum("age")
    # print g.max("age")
    # print g.min("age")
    # print g.avg("age")
    # print g.count("*")
    # print g.count("sex")
    t = Table("person")
    descs = ["id", "name", "age", "sex", "class"]
    raw_rows = [
        [1, "a", 10, "man", 1],
        [2, "b", 11, "man", 1],
        [3, "c", 10, "woman", 2],
        [4, "d", 13, "man", 2],
        [5, "张昱峰", 10, "man", "一班"],
        [6, "f", 17, "man", 1],
        [7, "g", 18, "woman", 2],
        [8, "h", 19, "man", 3],
        [9, "i", 30, "woman", 3],
        [10, "j", 20, "man", 3],
        [11, "k", 15, "man", 3],
    ]
    t.load_data(descs, raw_rows)
    t_descs = t.group_by("sex", "class").sum("age").count("*", alias="count").select("sex", "class", "name", "count",
                                                                                     "age__sum").desription
    t_rows = t.group_by("sex", "class").sum("age").count("*", alias="count").select("sex", "class", "name", "count",
                                                                                    "age__sum").fetchall()
    t_rows.insert(0, t_descs)
    pprint(t_rows)
