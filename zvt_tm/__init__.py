import functools

import zvt_tm.recorders as recorders
import zvt_tm.domain as domain


from zvt import init_config

zvt_tm_config = {}

int_zvt_tm_config = functools.partial(init_config, pkg_name='zvt_tm', current_config=zvt_tm_config)

int_zvt_tm_config()

__all__ = ['int_zvt_tm_config']