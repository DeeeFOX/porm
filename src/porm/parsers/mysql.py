from porm.errors import OperationalError


class ParsedResult(dict):
    def __init__(self, param: dict = None, filter: str = ''):
        super(ParsedResult, self).__init__(param=param, filter=filter)
        self._param = param
        self._filter = filter

    @property
    def param(self) -> dict:
        return self._param

    @property
    def filter(self) -> str:
        return self._filter


def parse_join(**terms) -> ParsedResult:
    """
    SQL JOIN条件解析接口
    :param terms: 其中的LIKE条件，需要用户在外层传入具体的模糊匹配符号例如经过处理的：{"name": ("%{}%".format(name), 'LIKE')}
    :return:
    """
    sql_params = {}
    term_sqls = ['1=1']
    for field_name, term in terms.items():
        # rename field name in fileter part to avoid conflict
        f_fname = u'joinfltr_{}'.format(field_name)
        if isinstance(term, (list, tuple)):
            operator = term[1]
            term = term[0]
            if isinstance(term, (list, tuple)):
                # range query
                if operator in ('IN', 'NOT IN'):
                    in_field_names = []
                    for idx, s in enumerate(term):
                        in_field_name = f_fname + str(idx)
                        in_field_names.append(u'%({in_field_name})s'.format(in_field_name=in_field_name))
                        sql_params[in_field_name] = s
                    if in_field_names:
                        term_sql = u"{field_name} {operator} ({f_fname})".format(
                            field_name=field_name,
                            operator=operator,
                            f_fname=', '.join(in_field_names)
                        )
                    else:
                        # IF IN an empty list return []
                        term_sql = u'1<>1'
                elif operator == 'LIKE':
                    like_term_sql = []
                    for idx, kw in enumerate(term):
                        kw = term[idx]
                        field_val_key = u"joinfltrLKE_{idx}_{fn}".format(idx=idx, fn=f_fname)
                        like_term_sql.append(u"{field_name} {op} %({field_val_key})s".format(
                            field_name=f_fname, op=operator, field_val_key=field_val_key
                        ))
                        sql_params[field_val_key] = u'%{}%'.format(kw)
                    term_sql = u' AND '.join(like_term_sql)
                else:
                    left_val = term[0]
                    left_op = operator[0]
                    right_val = term[1]
                    right_op = operator[1]
                    range_term_sql = []
                    if left_val:
                        if left_op == '(':
                            left_op = '>'
                        elif left_op == '[':
                            left_op = '>='
                        else:
                            raise OperationalError(u'Invalid Operator: {}'.format(operator))
                        left_field_name = f_fname + left_op
                        range_term_sql.append(u"{field_name}{op}%({left})s".format(
                            field_name=field_name, op=left_op, left=left_field_name))
                        sql_params[left_field_name] = left_val
                    if right_val:
                        if right_op == ')':
                            right_op = '<'
                        elif right_op == ']':
                            right_op = '<='
                        else:
                            raise OperationalError(u'Invalid Operator: {}'.format(operator))
                        right_field_name = f_fname + right_op
                        range_term_sql.append(u"{field_name}{op}%({right})s".format(
                            field_name=field_name, op=right_op, right=right_field_name))
                        sql_params[right_field_name] = right_val
                    term_sql = ' AND '.join(range_term_sql)
            else:
                # operator query
                if term is None:
                    continue
                elif term.startswith("\\") and term.endswith("\\"):
                    term_sql = u"{field_name}={term}".format(
                        field_name=field_name, term=term.replace("\\", "")
                    )
                else:
                    term_sql = u"{field_name} {operator} %({filter_field_name})s".format(
                        field_name=field_name, operator=operator, filter_field_name=f_fname)
                    sql_params[f_fname] = term
        else:
            if term is None:
                continue
            elif term.startswith("\\") and term.endswith("\\"):
                term_sql = u"{field_name}={term}".format(
                    field_name=field_name, term=term.replace("\\", "")
                )
            else:
                term_sql = u"{field_name}=%({filter_field_name})s".format(
                    field_name=field_name, filter_field_name=f_fname)
                sql_params[f_fname] = term
        term_sqls.append(term_sql)
    filters = ' AND '.join(term_sqls)
    return ParsedResult(param=sql_params, filter=filters)


def parse(tablename=None, order_by=None, page=None, size=None, **terms) -> ParsedResult:
    """
    SQL条件解析接口
    :param tablename:
    :param order_by:
    :param page:
    :param size:
    :param terms:
    :return: {
    'param': sql_params,
        'filter': filters
    }
    """
    sql_params = {}
    term_sqls = ['1=1']
    for fname, term in list(terms.items()):
        # rename field name in filter part to avoid conflict
        f_fname = u'fltr_{}'.format(fname)
        fname = u'{}.{}'.format(tablename, fname) if tablename else fname
        if isinstance(term, (list, tuple)):
            operator = term[1].strip()
            term = term[0]
            term_sql = u''
            if isinstance(term, (list, tuple)):
                # range query
                if operator in ('IN', 'NOT IN'):
                    in_field_names = []
                    for idx, s in enumerate(term):
                        in_field_name = f_fname + str(idx)
                        in_field_names.append(u'%({in_field_name})s'.format(in_field_name=in_field_name))
                        sql_params[in_field_name] = s
                    if in_field_names:
                        term_sql = u"{field_name} {operator} ({f_fname})".format(
                            field_name=fname,
                            operator=operator,
                            f_fname=', '.join(in_field_names)
                        )
                    else:
                        # IF IN an empty list return []
                        term_sql = u'1<>1'
                elif operator == 'LIKE':
                    like_term_sql = []
                    for idx, kw in enumerate(term):
                        kw = term[idx]
                        field_val_key = u"LKE_{idx}_{fn}".format(idx=idx, fn=fname)
                        like_term_sql.append(u"{field_name} {op} %({field_val_key})s".format(
                            field_name=fname, op=operator, field_val_key=field_val_key
                        ))
                        sql_params[field_val_key] = u'%{}%'.format(kw)
                    term_sql = u' AND '.join(like_term_sql)
                else:
                    left_val = term[0]
                    left_op = operator[0]
                    right_val = term[1]
                    right_op = operator[1]
                    range_term_sql = []
                    if left_val:
                        if left_op == '(':
                            left_op = '>'
                        elif left_op == '[':
                            left_op = '>='
                        else:
                            raise OperationalError(u'Invalid Operator: {}'.format(operator))
                        left_field_name = f_fname + left_op
                        range_term_sql.append(u"{field_name}{op}%({left})s".format(
                            field_name=fname, op=left_op, left=left_field_name))
                        sql_params[left_field_name] = left_val
                    if right_val:
                        if right_op == ')':
                            right_op = '<'
                        elif right_op == ']':
                            right_op = '<='
                        else:
                            raise OperationalError(u'Invalid Operator: {}'.format(operator))
                        right_field_name = f_fname + right_op
                        range_term_sql.append(u"{field_name}{op}%({right})s".format(
                            field_name=fname, op=right_op, right=right_field_name))
                        sql_params[right_field_name] = right_val
                    term_sql = ' AND '.join(range_term_sql)
            else:
                # operator query
                if term is None:
                    continue
                term_sql = u"{field_name} {operator} %({filter_field_name})s".format(
                    field_name=fname, operator=operator, filter_field_name=f_fname)
                sql_params[f_fname] = term
        else:
            if term is None:
                continue
            term_sql = u"{field_name}=%({filter_field_name})s".format(
                field_name=fname, filter_field_name=f_fname)
            sql_params[f_fname] = term
        if term_sql:
            term_sqls.append(term_sql)
        else:
            pass
    filters = ' AND '.join(term_sqls)
    if order_by:
        filters = u'{} ORDER BY {}'.format(filters, order_by)
    if page is not None and size is not None:
        size = max(1, int(size))
        filters = u'{} LIMIT %(page_from)s, %(page_to)s'.format(filters)
        sql_params['page_from'] = (max(0, int(page) - 1)) * size
        sql_params['page_to'] = size
    return ParsedResult(param=sql_params, filter=filters)
